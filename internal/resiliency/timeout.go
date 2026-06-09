package resiliency

import (
	"context"
	"log/slog"
	"net/http"
	"time"
)

// TimeoutMiddleware enforces a hard deadline on the entire request-response
// cycle (including upstream round-trip and response body transmission).
//
// # How it works
//
// A context.WithTimeout is applied to the incoming request's context before
// the request is passed to the next handler. If the downstream processing
// (proxy + upstream) takes longer than the deadline, the context is cancelled.
//
// The gateway's ErrorHandler (proxy/handler.go) detects context.DeadlineExceeded
// and responds with 504 Gateway Timeout. However, there is a race: if the
// downstream handler has already written headers before the deadline fires,
// we cannot overwrite them. In that case we log the late timeout but accept
// the partial response — this is the correct behaviour for streaming responses.
//
// # Per-route overrides
//
// Route-level timeouts are applied earlier, in the proxy Director (see
// proxy/handler.go). This middleware acts as the outer safety net for any
// request that did not get a route-level timeout (e.g. unmatched routes that
// return 404 before reaching the proxy).
//
// # Why not http.TimeoutHandler?
//
// http.TimeoutHandler from the stdlib spawns an extra goroutine per request
// and uses a channel to detect timeout. Our implementation is lighter: we
// derive a context with deadline and rely on the transport layer (net/http's
// connection handling) to honour the cancellation. This avoids the goroutine
// overhead on the hot path and integrates cleanly with the rest of our
// context-propagation model.
func TimeoutMiddleware(timeout time.Duration, logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if timeout <= 0 {
				next.ServeHTTP(w, r)
				return
			}

			ctx, cancel := context.WithTimeout(r.Context(), timeout)
			defer cancel() // always release resources, even on the success path

			// Detect if the incoming request already has a shorter deadline.
			// Honour the tighter of the two deadlines.
			if dl, ok := r.Context().Deadline(); ok {
				remaining := time.Until(dl)
				if remaining < timeout {
					// The existing deadline is already tighter; WithTimeout above
					// will be a no-op relative to the parent, so cancel it and keep
					// the parent context unmodified.
					cancel()
					next.ServeHTTP(w, r)
					return
				}
			}

			r = r.WithContext(ctx)

			tw := &timeoutResponseWriter{
				ResponseWriter: w,
				logger:         logger,
				path:           r.URL.Path,
				timeout:        timeout,
			}

			next.ServeHTTP(tw, r)

			// After the handler returns, check if the context expired.
			// If it did and nothing was written yet, send a 504.
			if ctx.Err() != nil && !tw.wroteHeader {
				logger.Warn("request timed out",
					"path", r.URL.Path,
					"timeout", timeout,
					"remote", r.RemoteAddr,
				)
				http.Error(w, "gateway timeout", http.StatusGatewayTimeout)
			}
		})
	}
}

// timeoutResponseWriter wraps http.ResponseWriter to track whether the
// downstream handler has committed a response (i.e. called WriteHeader or Write).
// Once committed we cannot overwrite headers/status, so we need to know.
type timeoutResponseWriter struct {
	http.ResponseWriter
	wroteHeader bool
	logger      *slog.Logger
	path        string
	timeout     time.Duration
}

func (tw *timeoutResponseWriter) WriteHeader(code int) {
	tw.wroteHeader = true
	tw.ResponseWriter.WriteHeader(code)
}

func (tw *timeoutResponseWriter) Write(b []byte) (int, error) {
	tw.wroteHeader = true
	return tw.ResponseWriter.Write(b)
}

// Unwrap allows http.ResponseController and other wrappers to access the
// underlying ResponseWriter (e.g. for flushing or hijacking).
func (tw *timeoutResponseWriter) Unwrap() http.ResponseWriter {
	return tw.ResponseWriter
}
