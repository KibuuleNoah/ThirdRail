package resiliency

import (
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

var ErrCircuitOpen = errors.New("circuit breaker open")

// current circuit breaker state.
type State int32

const (
	// normal operating state.
	// Failures are counted; if they reach FailureThreshold the breaker opens.
	StateClosed State = iota

	// rejects all requests immediately without attempting the call.
	// After OpenTimeout elapses the breaker transitions to StateHalfOpen.
	StateOpen

	// allows a single probe request through.
	// Success -> Closed; Failure -> Open (reset timer).	
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

type CircuitBreaker struct {
	name string

	failureThreshold uint32
	successThreshold uint32
	openTimeout      time.Duration

	// hot path reads must go through these.
	state         atomic.Int32
	failureCount  atomic.Uint32
	successCount  atomic.Uint32
	halfOpenProbe atomic.Bool // true while a probe is in flight in HalfOpen

	mu       sync.Mutex
	openedAt time.Time // protected by mu
}

type CircuitBreakerConfig struct {
	Name             string
	FailureThreshold uint32
	SuccessThreshold uint32
	OpenTimeout      time.Duration
}

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

/* 
Report whether the caller is permitted to attempt an operation.
Allow must be paired with a call to RecordSuccess or RecordFailure.
*/
func (cb *CircuitBreaker) Allow() error {
	switch State(cb.state.Load()) {
	case StateClosed:
		// allow 
		return nil

	case StateOpen:
		cb.mu.Lock()
		elapsed := time.Since(cb.openedAt)
		if elapsed < cb.openTimeout {
			cb.mu.Unlock()
			return fmt.Errorf("%w: upstream %q (retry after %s)",
				ErrCircuitOpen, cb.name, (cb.openTimeout-elapsed).Round(time.Millisecond))
		}
		// Switch to HalfOpen exactly one probe will be allowed.
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

/*
Notifies the breaker that the last operation succeeded.
*/
func (cb *CircuitBreaker) RecordSuccess() {
	switch State(cb.state.Load()) {
	case StateClosed:
		// reset the failure counter.
		cb.failureCount.Store(0)

	case StateHalfOpen:
		// increment success counter
		n := cb.successCount.Add(1)
		if n >= cb.successThreshold {
			// close the breaker once
			cb.toClosed()
		} else {
			// Release the probe slot so the next request can probe.
			cb.halfOpenProbe.Store(false)
		}
	}
}

/* 
RecordFailure notifies the breaker that the last operation failed.
*/

func (cb *CircuitBreaker) RecordFailure() {
	switch State(cb.state.Load()) {
	case StateClosed:
		// increments failure counter 
		n := cb.failureCount.Add(1)
		if n >= cb.failureThreshold {
		// open the breaker, threshold is reached.
			cb.toOpen()
		}

	case StateHalfOpen:
		// re-open the breaker (probe failed).
		cb.toOpen()
	}
}

func (cb *CircuitBreaker) CurrentState() State {
	return State(cb.state.Load())
}

func (cb *CircuitBreaker) Name() string { return cb.name }


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


/* Enforces the circuit breaker policy.
   Requests are rejected with 503 when the breaker is Open or when a HalfOpen
   probe is already in flight. Upstream errors (5xx) trip the breaker; 4xx and
   2xx/3xx responses are treated as successes.
  
   Important: this middleware must sit INSIDE the retry middleware so that
   individual retry attempts each pass through the breaker independently.
	 */
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

func isUpstreamError(code int) bool {
	return code >= 500
}

// captures the HTTP status code written by downstream handlers.
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
