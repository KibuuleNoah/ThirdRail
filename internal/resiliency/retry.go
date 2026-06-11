package resiliency

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"math"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"time"
)

type RetryConfig struct {
	MaxRetries int

	// Initial backoff duration before the first retry.
	BaseDelay time.Duration

	MaxDelay time.Duration

	// exponential growth factor applied after each failure.
	Multiplier float64

	// (optional) predicate that allows callers to control which
	// HTTP status codes trigger a retry. Defaults to retryOnDefault if nil.
	RetryOn func(statusCode int) bool
}

/*
retries on 502, 503, and 504 transient upstream errors.
429 (Too Many Requests) is excluded coz the rate limiter upstream
should handle that
*/
func retryOnDefault(code int) bool {
	return code == http.StatusBadGateway ||
		code == http.StatusServiceUnavailable ||
		code == http.StatusGatewayTimeout
}

/*retries the request on transient*/
func RetryMiddleware(cfg RetryConfig, logger *slog.Logger) func(http.Handler) http.Handler {
	if cfg.RetryOn == nil {
		cfg.RetryOn = retryOnDefault
	}
	if cfg.Multiplier <= 1 {
		cfg.Multiplier = 2.0
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Fast path, if retries are disabled, skip all buffering overhead.
			if cfg.MaxRetries == 0 {
				next.ServeHTTP(w, r)
				return
			}

			// Buffer the request body for replay.
			bodyBuf, bodyTooBig, err := bufferBody(r)
			if err != nil {
				http.Error(w, "failed to read request body", http.StatusInternalServerError)
				return
			}

			attempt := 0
			for {
				// Restore the body before each attempt.
				if bodyBuf != nil {
					r.Body = io.NopCloser(bytes.NewReader(bodyBuf))
					r.ContentLength = int64(len(bodyBuf))
				}

				// Capture the response so we can inspect the status code.
				rec := httptest.NewRecorder()
				next.ServeHTTP(rec, r)

				status := rec.Code

				// Decide whether to retry.
				shouldRetry := !bodyTooBig &&
					attempt < cfg.MaxRetries &&
					cfg.RetryOn(status)

				if !shouldRetry {
					// Flush the captured response to the real writer.
					copyRecordedResponse(w, rec)
					return
				}

				attempt++
				delay := jitteredDelay(cfg.BaseDelay, cfg.MaxDelay, cfg.Multiplier, attempt)

				logger.Warn("retrying request",
					"attempt", attempt,
					"max_retries", cfg.MaxRetries,
					"status", status,
					"delay", delay,
					"path", r.URL.Path,
				)

				// Honour context cancellation before sleeping.
				select {
				case <-r.Context().Done():
					http.Error(w, "request cancelled", http.StatusServiceUnavailable)
					return
				case <-time.After(delay):
				}

				// Re-check context after the sleep (the timer may have fired
				// simultaneous with a cancellation).
				if err := r.Context().Err(); err != nil {
					if errors.Is(err, context.DeadlineExceeded) {
						http.Error(w, "gateway timeout", http.StatusGatewayTimeout)
					} else {
						http.Error(w, "request cancelled", http.StatusServiceUnavailable)
					}
					return
				}
			}
		})
	}
}


const maxBodyBuffer = 1 << 20 // 1 MiB

/* 
reads and buffers the request body up to maxBodyBuffer bytes.
 returns:
   - buf -> the buffered bytes (nil if the body was empty or nil)
   - tooBig-> true if the body exceeded the cap (not buffered; retries disabled)
   - err-> any I/O error during reading
*/
func bufferBody(r *http.Request) (buf []byte, tooBig bool, err error) {
	if r.Body == nil || r.ContentLength == 0 {
		return nil, false, nil
	}
	if r.ContentLength > maxBodyBuffer {
		return nil, true, nil
	}

	// Read up to maxBodyBuffer+1 to detect oversized bodies with a single read.
	limited := &io.LimitedReader{R: r.Body, N: maxBodyBuffer + 1}
	data, readErr := io.ReadAll(limited)
	_ = r.Body.Close()

	if readErr != nil {
		return nil, false, readErr
	}
	if int64(len(data)) > maxBodyBuffer {
		return nil, true, nil
	}
	return data, false, nil
}

/*
computes a full-jitter backoff duration.
*/
func jitteredDelay(base, maxDelay time.Duration, multiplier float64, attempt int) time.Duration {

	cap := float64(base) * math.Pow(multiplier, float64(attempt))
	if cap > float64(maxDelay) {
		cap = float64(maxDelay)
	}
	
	return time.Duration(rand.Float64() * cap)
}

/* 
writes the headers and body from an httptest.Recorder
 back to the real http.ResponseWriter.
 */
func copyRecordedResponse(w http.ResponseWriter, rec *httptest.ResponseRecorder) {
	dst := w.Header()
	for k, vs := range rec.Header() {
		dst[k] = vs
	}
	w.WriteHeader(rec.Code)
	_, _ = rec.Body.WriteTo(w)
}
