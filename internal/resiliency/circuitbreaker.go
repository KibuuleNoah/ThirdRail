// Package resiliency provides fault-tolerance primitives for the ThirdRail gateway.
package resiliency

import (
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// ErrCircuitOpen is returned when a request is rejected because the circuit
// breaker is in the Open state.
var ErrCircuitOpen = errors.New("circuit breaker open")

// State represents the circuit breaker's current state.
type State int32

const (
	// StateClosed is the normal operating state. Requests flow through.
	// Failures are counted; if they reach FailureThreshold the breaker opens.
	StateClosed State = iota

	// StateOpen rejects all requests immediately without attempting the call.
	// After OpenTimeout elapses the breaker transitions to StateHalfOpen.
	StateOpen

	// StateHalfOpen allows a single probe request through.
	// Success → Closed; Failure → Open (reset timer).
	StateHalfOpen
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "Closed"
	case StateOpen:
		return "Open"
	case StateHalfOpen:
		return "HalfOpen"
	default:
		return fmt.Sprintf("Unknown(%d)", int(s))
	}
}

// CircuitBreaker implements a per-upstream three-state circuit breaker.
//
// Concurrency model:
//   - state and failure/success counters are stored as atomics for lock-free
//     reads on the critical path (Allow).
//   - mu is only acquired for state transitions and openedAt access, which are
//     rare events. This keeps Allow() contention-free under normal (Closed) load.
type CircuitBreaker struct {
	name string

	failureThreshold uint32
	successThreshold uint32
	openTimeout      time.Duration

	// Atomics — hot path reads must go through these.
	state         atomic.Int32
	failureCount  atomic.Uint32
	successCount  atomic.Uint32
	halfOpenProbe atomic.Bool // true while a probe is in flight in HalfOpen

	mu       sync.Mutex
	openedAt time.Time // protected by mu
}

// CircuitBreakerConfig carries constructor parameters.
type CircuitBreakerConfig struct {
	Name             string
	FailureThreshold uint32
	SuccessThreshold uint32
	OpenTimeout      time.Duration
}

// NewCircuitBreaker constructs a CircuitBreaker starting in StateClosed.
func NewCircuitBreaker(cfg CircuitBreakerConfig) *CircuitBreaker {
	cb := &CircuitBreaker{
		name:             cfg.Name,
		failureThreshold: cfg.FailureThreshold,
		successThreshold: cfg.SuccessThreshold,
		openTimeout:      cfg.OpenTimeout,
	}
	cb.state.Store(int32(StateClosed))
	return cb
}

// Allow reports whether the caller is permitted to attempt an operation.
//
//   - StateClosed   → always nil (allowed).
//   - StateOpen     → ErrCircuitOpen, unless OpenTimeout has elapsed, in which
//     case the breaker transitions to HalfOpen and allows one probe.
//   - StateHalfOpen → nil only for the first caller (the probe); all others
//     get ErrCircuitOpen until the probe resolves.
//
// Allow must be paired with a call to RecordSuccess or RecordFailure.
func (cb *CircuitBreaker) Allow() error {
	switch State(cb.state.Load()) {
	case StateClosed:
		return nil

	case StateOpen:
		cb.mu.Lock()
		elapsed := time.Since(cb.openedAt)
		if elapsed < cb.openTimeout {
			cb.mu.Unlock()
			return fmt.Errorf("%w: upstream %q (retry after %s)",
				ErrCircuitOpen, cb.name, (cb.openTimeout-elapsed).Round(time.Millisecond))
		}
		// Transition to HalfOpen — exactly one probe will be allowed.
		cb.state.Store(int32(StateHalfOpen))
		cb.halfOpenProbe.Store(false)
		cb.mu.Unlock()
		fallthrough // evaluate HalfOpen logic immediately

	case StateHalfOpen:
		// Only the goroutine that flips false→true gets to probe.
		if cb.halfOpenProbe.CompareAndSwap(false, true) {
			return nil
		}
		return fmt.Errorf("%w: upstream %q (half-open probe in flight)", ErrCircuitOpen, cb.name)
	}

	return nil
}

// RecordSuccess notifies the breaker that the last operation succeeded.
//
//   - StateClosed:   resets the failure counter.
//   - StateHalfOpen: increments success counter; closes the breaker once
//     SuccessThreshold consecutive successes are recorded.
func (cb *CircuitBreaker) RecordSuccess() {
	switch State(cb.state.Load()) {
	case StateClosed:
		cb.failureCount.Store(0)

	case StateHalfOpen:
		n := cb.successCount.Add(1)
		if n >= cb.successThreshold {
			cb.toClosed()
		} else {
			// Release the probe slot so the next request can probe.
			cb.halfOpenProbe.Store(false)
		}
	}
}

// RecordFailure notifies the breaker that the last operation failed.
//
//   - StateClosed:   increments failure counter; opens the breaker when threshold is reached.
//   - StateHalfOpen: immediately re-opens the breaker (probe failed).
func (cb *CircuitBreaker) RecordFailure() {
	switch State(cb.state.Load()) {
	case StateClosed:
		n := cb.failureCount.Add(1)
		if n >= cb.failureThreshold {
			cb.toOpen()
		}

	case StateHalfOpen:
		cb.toOpen()
	}
}

// CurrentState returns the current circuit state. Safe for concurrent use.
func (cb *CircuitBreaker) CurrentState() State {
	return State(cb.state.Load())
}

// Name returns the breaker's identifier.
func (cb *CircuitBreaker) Name() string { return cb.name }

// ─── state transitions ────────────────────────────────────────────────────────

func (cb *CircuitBreaker) toOpen() {
	cb.mu.Lock()
	cb.openedAt = time.Now()
	cb.mu.Unlock()

	cb.state.Store(int32(StateOpen))
	cb.failureCount.Store(0)
	cb.successCount.Store(0)
}

func (cb *CircuitBreaker) toClosed() {
	cb.state.Store(int32(StateClosed))
	cb.failureCount.Store(0)
	cb.successCount.Store(0)
	cb.halfOpenProbe.Store(false)
}

// ─── HTTP middleware ──────────────────────────────────────────────────────────

// Middleware wraps an http.Handler and enforces the circuit breaker policy.
// Requests are rejected with 503 when the breaker is Open or when a HalfOpen
// probe is already in flight. Upstream errors (5xx) trip the breaker; 4xx and
// 2xx/3xx responses are treated as successes.
//
// Important: this middleware must sit INSIDE the retry middleware so that
// individual retry attempts each pass through the breaker independently.
func (cb *CircuitBreaker) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := cb.Allow(); err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}

		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)

		if isUpstreamError(rec.status) {
			cb.RecordFailure()
		} else {
			cb.RecordSuccess()
		}
	})
}

// isUpstreamError returns true for status codes that represent upstream
// failures (5xx). 4xx errors are client faults and should not trip the breaker.
func isUpstreamError(code int) bool {
	return code >= 500
}

// statusRecorder captures the HTTP status code written by downstream handlers.
// It embeds ResponseWriter so all other methods (Header, Write) pass through.
type statusRecorder struct {
	http.ResponseWriter
	status  int
	written bool
}

func (r *statusRecorder) WriteHeader(code int) {
	if !r.written {
		r.status = code
		r.written = true
	}
	r.ResponseWriter.WriteHeader(code)
}

// Write captures status 200 implicitly when WriteHeader was never called.
func (r *statusRecorder) Write(b []byte) (int, error) {
	if !r.written {
		r.status = http.StatusOK
		r.written = true
	}
	return r.ResponseWriter.Write(b)
}
