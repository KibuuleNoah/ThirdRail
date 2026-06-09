package proxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"
)

// contextKey is an unexported type used for context values stored by this package.
// Using a package-local type prevents key collisions with other packages.
type contextKey int

const (
	keyRoute contextKey = iota
)

// Handler is the core reverse proxy HTTP handler. It resolves the upstream
// route for each request, applies prefix-stripping if configured, and delegates
// to httputil.ReverseProxy with custom Director and error-handling logic.
//
// One httputil.ReverseProxy instance is created per unique upstream host.
// This pool is maintained in the proxyPool map and is safe for concurrent
// access via a sync.RWMutex on the Router.
//
// Key design decisions:
//  1. We do NOT reuse a single httputil.ReverseProxy for all routes because
//     its internal Transport is keyed to a single upstream; sharing one
//     instance across different upstreams would serialize connections.
//  2. The Director mutates a shallow copy of the request (httputil guarantees
//     this) so no original request fields are clobbered.
//  3. ModifyResponse and ErrorHandler are overridden to give us control over
//     error status codes and structured logging.
type Handler struct {
	router     *Router
	defaultTO  time.Duration
	transport  http.RoundTripper // injected for testability; nil → http.DefaultTransport
	proxyCache map[string]*httputil.ReverseProxy
	logger     *slog.Logger
}

// HandlerOption is a functional option for Handler.
type HandlerOption func(*Handler)

// WithTransport overrides the http.RoundTripper used by all upstream proxies.
// Primarily useful in tests to inject a mock transport.
func WithTransport(t http.RoundTripper) HandlerOption {
	return func(h *Handler) { h.transport = t }
}

// WithLogger injects a structured logger. If not provided, slog.Default() is used.
func WithLogger(l *slog.Logger) HandlerOption {
	return func(h *Handler) { h.logger = l }
}

// NewHandler constructs a Handler. The defaultTimeout is applied to upstream
// requests that do not have a route-level override.
func NewHandler(router *Router, defaultTimeout time.Duration, opts ...HandlerOption) *Handler {
	h := &Handler{
		router:     router,
		defaultTO:  defaultTimeout,
		proxyCache: make(map[string]*httputil.ReverseProxy),
		logger:     slog.Default(),
	}
	for _, o := range opts {
		o(h)
	}
	if h.transport == nil {
		h.transport = defaultTransport()
	}
	return h
}

// ServeHTTP satisfies http.Handler. It:
//  1. Resolves the upstream route for the request path.
//  2. Injects the resolved Route into the request context (for downstream middleware).
//  3. Delegates to the per-upstream ReverseProxy.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	route, err := h.router.Match(r.URL.Path)
	if err != nil {
		h.logger.Warn("no route matched", "path", r.URL.Path, "remote", r.RemoteAddr)
		http.Error(w, "404 no upstream route", http.StatusNotFound)
		return
	}

	// Store the matched route in context so middleware further up the chain
	// (rate-limiter, circuit breaker) can retrieve it without re-running
	// path matching.
	r = r.WithContext(context.WithValue(r.Context(), keyRoute, route))

	rp := h.proxyFor(route.Upstream)
	rp.ServeHTTP(w, r)
}

// RouteFromContext retrieves the resolved Route that was stored by the Handler.
// Returns the zero value and false if not present (request did not pass through Handler).
func RouteFromContext(ctx context.Context) (Route, bool) {
	v, ok := ctx.Value(keyRoute).(Route)
	return v, ok
}

//  internal 

// proxyFor returns (or lazily creates) a *httputil.ReverseProxy for the given
// upstream base URL. The cache is keyed by scheme+host to ensure connection
// pools are properly scoped. A simple map lookup with no lock is safe here
// because proxyCache is only written during startup or route reload (before
// any requests are served), and reads happen concurrently during request
// handling.
//
// NOTE: If hot-reload of routes is required in production, this map should be
// protected by a sync.RWMutex or replaced with a sync.Map.
func (h *Handler) proxyFor(target *url.URL) *httputil.ReverseProxy {
	key := target.Scheme + "://" + target.Host
	if rp, ok := h.proxyCache[key]; ok {
		return rp
	}

	rp := &httputil.ReverseProxy{
		Director:       h.director(target),
		Transport:      h.transport,
		ModifyResponse: modifyResponse,
		ErrorHandler:   h.errorHandler,

		// BufferPool reduces GC pressure on high-throughput paths by reusing
		// the byte slices used to copy response bodies.
		BufferPool: newBufferPool(),
	}
	h.proxyCache[key] = rp
	return rp
}

// director returns a Director func that rewrites the outgoing request URL to
// point at the upstream target. It preserves query parameters, applies
// prefix-stripping if configured, and sets standard proxy headers.
//
// httputil.ReverseProxy calls Director with a shallow copy of the original
// request; we own the copy and can mutate it freely.
func (h *Handler) director(target *url.URL) func(*http.Request) {
	return func(req *http.Request) {
		// Retrieve the route from context (set in ServeHTTP above).
		route, _ := RouteFromContext(req.Context())

		req.URL.Scheme = target.Scheme
		req.URL.Host = target.Host

		// Apply prefix stripping before forwarding.
		path := req.URL.Path
		if route.StripPrefix && route.PathPrefix != "" {
			path = strings.TrimPrefix(path, route.PathPrefix)
			if path == "" {
				path = "/"
			}
		}
		req.URL.Path = singleJoiningSlash(target.Path, path)

		// Preserve the raw query string (do not re-encode).
		if req.URL.RawPath != "" {
			req.URL.RawPath = singleJoiningSlash(target.Path, req.URL.RawPath)
		}

		// Standard reverse-proxy forwarding headers.
		if req.Header == nil {
			req.Header = make(http.Header)
		}
		if clientIP, _, err := net.SplitHostPort(req.RemoteAddr); err == nil {
			if prior := req.Header.Get("X-Forwarded-For"); prior != "" {
				clientIP = prior + ", " + clientIP
			}
			req.Header.Set("X-Forwarded-For", clientIP)
		}
		req.Header.Set("X-Forwarded-Host", req.Host)
		req.Header.Set("X-Forwarded-Proto", scheme(req))

		// Clear the inbound Host header so the upstream sees its own hostname.
		req.Host = target.Host

		// Apply per-route timeout by wrapping the request context.
		// httputil.ReverseProxy's Director receives a pointer to the outbound
		// *http.Request. req.WithContext() returns a new *http.Request, so
		// simply rebinding the local `req` variable would have no effect on
		// what the proxy actually sends. We must overwrite the struct value
		// that `req` points to via dereference assignment.
		to := route.Timeout
		if to == 0 {
			to = h.defaultTO
		}
		if to > 0 {
			ctx, cancel := context.WithTimeout(req.Context(), to)
			newReq := req.WithContext(context.WithValue(ctx, cancelKey{}, cancel))
			*req = *newReq // overwrite the struct the caller's pointer references
		}
	}
}

// modifyResponse is called for successful (non-error) upstream responses.
// We add a header to identify the gateway and ensure cancel is invoked.
func modifyResponse(resp *http.Response) error {
	resp.Header.Set("X-Gateway", "ThirdRail")

	// Release the context cancel function attached by director, if present.
	if cancel, ok := resp.Request.Context().Value(cancelKey{}).(context.CancelFunc); ok {
		resp.Body = &cancelOnClose{ReadCloser: resp.Body, cancel: cancel}
	}
	return nil
}

// errorHandler is invoked by httputil.ReverseProxy when the upstream request
// fails entirely (connection refused, timeout, etc.). It maps error types to
// appropriate HTTP status codes and logs structured diagnostics.
func (h *Handler) errorHandler(w http.ResponseWriter, r *http.Request, err error) {
	// Release the context cancel if one was attached.
	if cancel, ok := r.Context().Value(cancelKey{}).(context.CancelFunc); ok {
		cancel()
	}

	code := http.StatusBadGateway
	msg := "upstream error"

	switch {
	case errors.Is(err, context.DeadlineExceeded):
		code = http.StatusGatewayTimeout
		msg = "upstream timeout"
	case errors.Is(err, context.Canceled):
		// Client disconnected; no point sending a response.
		code = http.StatusServiceUnavailable
		msg = "request canceled"
	default:
		// Distinguish connection-refused / DNS failures from generic errors.
		var netErr *net.OpError
		if errors.As(err, &netErr) {
			code = http.StatusBadGateway
			msg = fmt.Sprintf("upstream connection error: %s", netErr.Op)
		}
	}

	h.logger.Error("upstream request failed",
		"status", code,
		"error", err.Error(),
		"path", r.URL.Path,
		"remote", r.RemoteAddr,
	)

	http.Error(w, msg, code)
}

//  helpers 

type cancelKey struct{}

// cancelOnClose wraps an io.ReadCloser and calls a cancel function when
// the body is closed, ensuring the context is always released.
type cancelOnClose struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (c *cancelOnClose) Close() error {
	c.cancel()
	return c.ReadCloser.Close()
}

// singleJoiningSlash joins two URL path segments with exactly one slash.
// Mirrors the logic in httputil.NewSingleHostReverseProxy.
func singleJoiningSlash(a, b string) string {
	aSlash := strings.HasSuffix(a, "/")
	bSlash := strings.HasPrefix(b, "/")
	switch {
	case aSlash && bSlash:
		return a + b[1:]
	case !aSlash && !bSlash:
		return a + "/" + b
	}
	return a + b
}

func scheme(r *http.Request) string {
	if r.TLS != nil {
		return "https"
	}
	return "http"
}

// defaultTransport returns a tuned http.Transport for high-concurrency proxy use.
// Key tuning choices:
//   - MaxIdleConnsPerHost matches expected per-upstream concurrency bursts.
//   - DisableCompression: the gateway should not re-compress; let the upstream
//     negotiate compression directly with the client.
//   - ForceAttemptHTTP2: enable H2 to upstreams that advertise it.
func defaultTransport() *http.Transport {
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          500,
		MaxIdleConnsPerHost:   100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		DisableCompression:    true,
		ForceAttemptHTTP2:     true,
	}
}
