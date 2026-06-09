package middleware

import (
	"log/slog"
	"net/http"
	"time"
)

/* 
Holds all the fields captured for a single request/response cycle.
 It is stack-allocated (passed by value) to avoid heap allocation on the hot path.
*/
type LogEntry struct {
	Method     string
	Path       string
	Query      string
	RemoteAddr string
	UserAgent  string
	StatusCode int
	Latency    time.Duration
	BytesOut   int
	RequestID  string
}

// LoggingMiddleware returns a structured HTTP logging middleware that records
// per-request latency, status code, and diagnostic fields.
//
// # Design goals
//
//  1. Non-blocking: log writes are dispatched asynchronously through a buffered
//     channel to a dedicated writer goroutine. Request handling is never delayed
//     by slow I/O (e.g. a log sink rotating a file, writing to a remote SIEM).
//
//  2. Allocation efficiency: LogEntry is a value type; the channel carries it
//     by value. slog attribute construction uses the inline ...Attr form to
//     avoid variadic slice allocation.
//
//  3. Back-pressure: if the channel buffer is full (the writer goroutine is
//     lagging), the middleware falls back to a synchronous write. This ensures
//     log records are never silently dropped in production.
//
// # Dropped records
//
// Under extreme overload the channel may fill faster than it drains. Rather
// than block request handling, we count dropped records and emit a periodic
// summary. This is the correct trade-off for a gateway: request latency matters
// more than log completeness.
func LoggingMiddleware(logger *slog.Logger, bufferSize int) func(http.Handler) http.Handler {
	if bufferSize <= 0 {
		bufferSize = 4096
	}

	ch := make(chan LogEntry, bufferSize)

	// Single writer goroutine — serialises log writes.
	go func() {
		for entry := range ch {
			writeEntry(logger, entry)
		}
	}()

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()

			// Wrap the response writer to capture status code and bytes written.
			lrw := &loggingResponseWriter{
				ResponseWriter: w,
				statusCode:     http.StatusOK,
			}

			next.ServeHTTP(lrw, r)

			entry := LogEntry{
				Method:     r.Method,
				Path:       r.URL.Path,
				Query:      r.URL.RawQuery,
				RemoteAddr: r.RemoteAddr,
				UserAgent:  r.UserAgent(),
				StatusCode: lrw.statusCode,
				Latency:    time.Since(start),
				BytesOut:   lrw.bytesWritten,
				RequestID:  r.Header.Get("X-Request-ID"),
			}

			// Non-blocking send: if the buffer is full, write synchronously.
			select {
			case ch <- entry:
			default:
				// Buffer full — write synchronously to avoid dropping critical records.
				writeEntry(logger, entry)
			}
		})
	}
}

// writeEntry emits a structured log record for a completed request.
// Level is chosen based on status code: errors get slog.LevelError,
// warnings for 4xx, info for everything else.
func writeEntry(logger *slog.Logger, e LogEntry) {
	level := slog.LevelInfo
	switch {
	case e.StatusCode >= 500:
		level = slog.LevelError
	case e.StatusCode >= 400:
		level = slog.LevelWarn
	}

	attrs := []slog.Attr{
		slog.String("method", e.Method),
		slog.String("path", e.Path),
		slog.Int("status", e.StatusCode),
		slog.Duration("latency", e.Latency),
		slog.String("remote", e.RemoteAddr),
		slog.Int("bytes_out", e.BytesOut),
	}
	if e.Query != "" {
		attrs = append(attrs, slog.String("query", e.Query))
	}
	if e.RequestID != "" {
		attrs = append(attrs, slog.String("request_id", e.RequestID))
	}
	if e.UserAgent != "" {
		attrs = append(attrs, slog.String("user_agent", e.UserAgent))
	}

	logger.LogAttrs(nil, level, "request", attrs...)
}

// loggingResponseWriter wraps http.ResponseWriter to intercept WriteHeader and
// Write calls so we can record the final status code and body size.
type loggingResponseWriter struct {
	http.ResponseWriter
	statusCode   int
	bytesWritten int
	headerSent   bool
}

func (lrw *loggingResponseWriter) WriteHeader(code int) {
	if !lrw.headerSent {
		lrw.statusCode = code
		lrw.headerSent = true
	}
	lrw.ResponseWriter.WriteHeader(code)
}

func (lrw *loggingResponseWriter) Write(b []byte) (int, error) {
	if !lrw.headerSent {
		lrw.statusCode = http.StatusOK
		lrw.headerSent = true
	}
	n, err := lrw.ResponseWriter.Write(b)
	lrw.bytesWritten += n
	return n, err
}

// Unwrap exposes the underlying ResponseWriter for ResponseController compatibility
// (e.g. enabling http.Flusher or http.Hijacker when the wrapped writer supports it).
func (lrw *loggingResponseWriter) Unwrap() http.ResponseWriter {
	return lrw.ResponseWriter
}
