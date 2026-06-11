package resiliency

import (
	"context"
	"log/slog"
	"net/http"
	"time"
)


func TimeoutMiddleware(timeout time.Duration, logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if timeout <= 0 {
				next.ServeHTTP(w, r)
				return
			}

			ctx, cancel := context.WithTimeout(r.Context(), timeout)
			defer cancel() // always release

			// Detect if incoming request already has a shorter deadline.
			if dl, ok := r.Context().Deadline(); ok {
				remaining := time.Until(dl)
				if remaining < timeout {
					// The existing deadline is already tighter
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

			// check if the context expired.
			if ctx.Err() != nil && !tw.wroteHeader {
				logger.Warn("request timed out",
					"path", r.URL.Path,
					"timeout", timeout,
					"remote", r.RemoteAddr,
				)
			// context expired & nothing was written yet, send a 504.
				http.Error(w, "gateway timeout", http.StatusGatewayTimeout)
			}
		})
	}
}

/* 
Track whether the downstream handler has committed a response (i.e. called WriteHeader or Write).
 Once committed we cannot overwrite headers/status, so we need to know.
 */
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

/* 
Allows http.ResponseController and other wrappers to access the
 ResponseWriter (e.g. for flushing or hijacking).
 */
func (tw *timeoutResponseWriter) Unwrap() http.ResponseWriter {
	return tw.ResponseWriter
}
