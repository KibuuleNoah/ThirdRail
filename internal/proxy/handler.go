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

type contextKey int

const (
	keyRoute contextKey = iota
)

type Handler struct {
	router     *Router
	defaultTO  time.Duration
	transport  http.RoundTripper
	proxyCache map[string]*httputil.ReverseProxy
	logger     *slog.Logger
}

type HandlerOption func(*Handler)

/* 
Overrides the http.RoundTripper used by all upstream proxies.
Primarily useful in tests to inject a mock transport.
*/
func WithTransport(t http.RoundTripper) HandlerOption {
	return func(h *Handler) { h.transport = t }
}

/* 
Injects a structured logger. If not provided, slog.Default() is used.
*/
func WithLogger(l *slog.Logger) HandlerOption {
	return func(h *Handler) { h.logger = l }
}

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

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	route, err := h.router.Match(r.URL.Path)
	if err != nil {
		h.logger.Warn("no route matched", "path", r.URL.Path, "remote", r.RemoteAddr)
		http.Error(w, "404 no upstream route", http.StatusNotFound)
		return
	}

	// cache matched route
	r = r.WithContext(context.WithValue(r.Context(), keyRoute, route))

	rp := h.proxyFor(route.Upstream)
	rp.ServeHTTP(w, r)
}

/* 
Retrieves the resolved Route stored Handler.
*/
func RouteFromContext(ctx context.Context) (Route, bool) {
	v, ok := ctx.Value(keyRoute).(Route)
	return v, ok
}


/*
returns a *httputil.ReverseProxy for the given upstream base URL. 
*/
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

		// reduce GC pressure on high-throughput paths
		BufferPool: newBufferPool(),
	}
	h.proxyCache[key] = rp
	return rp
}

/*
returns a func that rewrites the outgoing request URL to
 point at the upstream target.
 */
func (h *Handler) director(target *url.URL) func(*http.Request) {
	return func(req *http.Request) {
		// Retrieve the route from context.
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

		// Preserve the raw query string.
		if req.URL.RawPath != "" {
			req.URL.RawPath = singleJoiningSlash(target.Path, req.URL.RawPath)
		}

		// reverse-proxy forwarding headers.
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

		// Clear the inbound Host header.
		req.Host = target.Host

		// Set per-route timeout
		to := route.Timeout
		if to == 0 {
			to = h.defaultTO
		}
		if to > 0 {
			ctx, cancel := context.WithTimeout(req.Context(), to)
			newReq := req.WithContext(context.WithValue(ctx, cancelKey{}, cancel))
			*req = *newReq
		}
	}
}

func modifyResponse(resp *http.Response) error {
	resp.Header.Set("X-Gateway", "ThirdRail")

	// Release the context cancel if one was attached.
	if cancel, ok := resp.Request.Context().Value(cancelKey{}).(context.CancelFunc); ok {
		resp.Body = &cancelOnClose{ReadCloser: resp.Body, cancel: cancel}
	}
	return nil
}

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
		// Client disconnected, cancel response.
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


type cancelKey struct{}

type cancelOnClose struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (c *cancelOnClose) Close() error {
	c.cancel()
	return c.ReadCloser.Close()
}

// merges two URL path segments.
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

// high-concurrency proxy use.
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
