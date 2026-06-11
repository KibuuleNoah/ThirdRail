package middleware

import (
	"log/slog"
	"math"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// ============================================================================================
//  Token Bucket 
// ============================================================================================

// 1 token = 1_000_000 internal units
const tokenScale = 1_000_000

/* 
Implements a token-bucket rate limiter.
*/
type tokenBucket struct {
	tokens   atomic.Int64
	capacity int64
}

func newTokenBucket(rps float64, burst int) *tokenBucket {
	cap := int64(burst) * tokenScale
	tb := &tokenBucket{capacity: cap}
	tb.tokens.Store(cap) // start full

	// Refill goroutine — runs for the lifetime of the gateway.
	// Tick interval of 10 ms provides smooth refill without wasting CPU.
	// At 1 000 RPS a 10 ms tick adds 10 tokens per interval.
	tickInterval := 10 * time.Millisecond
	tokensPerTick := int64(math.Round(rps * float64(tickInterval) / float64(time.Second) * tokenScale))

	go func() {
		ticker := time.NewTicker(tickInterval)
		defer ticker.Stop()
		for range ticker.C {
			for {
				cur := tb.tokens.Load()
				next := cur + tokensPerTick
				if next > cap {
					next = cap
				}
				if tb.tokens.CompareAndSwap(cur, next) {
					break
				}
				// CAS failed → another goroutine raced; retry immediately.
			}
		}
	}()

	return tb
}

// allow attempts to consume one token. Returns true if allowed, false if the
// bucket is empty. This is the hot path — it must be allocation-free.
func (tb *tokenBucket) allow() bool {
	for {
		cur := tb.tokens.Load()
		if cur < tokenScale {
			return false // bucket empty
		}
		if tb.tokens.CompareAndSwap(cur, cur-tokenScale) {
			return true
		}
		// Another goroutine consumed a token between our Load and CAS; retry.
	}
}
// ============================================================================================
//  Per-key limiter 
// ============================================================================================

/* 
Maintains a separate token bucket per key (e.g. client IP or API key). 
 Buckets are created lazily and evicted after a configurable TTL
 to prevent unbounded memory growth from abandoned keys.
 */
type keyedLimiter struct {
	mu      sync.Mutex
	buckets map[string]*bucketEntry
	rps     float64
	burst   int
	ttl     time.Duration
}

type bucketEntry struct {
	bucket  *tokenBucket
	lastSee time.Time
}

func newKeyedLimiter(rps float64, burst int, ttl time.Duration) *keyedLimiter {
	kl := &keyedLimiter{
		buckets: make(map[string]*bucketEntry),
		rps:     rps,
		burst:   burst,
		ttl:     ttl,
	}
	// Background eviction to prevent memory growth. Runs every TTL interval.
	go kl.evictLoop()
	return kl
}

// allow checks whether the request identified by key is within rate limits.
func (kl *keyedLimiter) allow(key string) bool {
	kl.mu.Lock()
	entry, ok := kl.buckets[key]
	if !ok {
		entry = &bucketEntry{bucket: newTokenBucket(kl.rps, kl.burst)}
		kl.buckets[key] = entry
	}
	entry.lastSee = time.Now()
	bucket := entry.bucket
	kl.mu.Unlock()

	return bucket.allow()
}

func (kl *keyedLimiter) evictLoop() {
	ticker := time.NewTicker(kl.ttl)
	defer ticker.Stop()
	for range ticker.C {
		kl.evict()
	}
}

func (kl *keyedLimiter) evict() {
	threshold := time.Now().Add(-kl.ttl)
	kl.mu.Lock()
	for k, e := range kl.buckets {
		if e.lastSee.Before(threshold) {
			delete(kl.buckets, k)
		}
	}
	kl.mu.Unlock()
}

// ============================================================================================
// ============================================================================================

/* 
Configures the rate-limiting middleware.
*/
type RateLimiterConfig struct {
	RequestsPerSecond float64

	// maximum token accumulation (allows short bursts).
	BurstSize int

	// when true enforces limits per client IP rather than globally.
	PerIP bool

	// extracts a rate-limit key from the request. Defaults to client IP.
	KeyFunc func(r *http.Request) string

	// controls how long an idle per-key bucket is kept before eviction.
	// Only used when PerIP is true. Defaults to 5 minutes.
	BucketTTL time.Duration
}

// ============================================================================================
//  Middleware 
// ============================================================================================


// RateLimiterMiddleware returns an http.Handler middleware that enforces
// token-bucket rate limiting.
//
// Global mode (cfg.PerIP == false):
//
//	A single token bucket is shared across all requests. This protects the
//	gateway's own resources and any single upstream from being overwhelmed.
//
// Per-key mode (cfg.PerIP == true or cfg.KeyFunc set):
//
//	Each unique key gets its own bucket. Useful for tenant-level isolation.
//	The KeyFunc defaults to extracting the client IP.
//
// Responses:
//
//	429 Too Many Requests with Retry-After and X-RateLimit-* headers.
func RateLimiterMiddleware(cfg RateLimiterConfig, logger *slog.Logger) func(http.Handler) http.Handler {
	// Apply defaults.
	if cfg.BucketTTL == 0 {
		cfg.BucketTTL = 5 * time.Minute
	}
	if cfg.KeyFunc == nil {
		cfg.KeyFunc = clientIP
	}

	var (
		globalBucket *tokenBucket
		perKeyLim    *keyedLimiter
	)

	if cfg.PerIP || cfg.KeyFunc != nil {
		perKeyLim = newKeyedLimiter(cfg.RequestsPerSecond, cfg.BurstSize, cfg.BucketTTL)
	} else {
		globalBucket = newTokenBucket(cfg.RequestsPerSecond, cfg.BurstSize)
	}

	retryAfterSecs := int(math.Ceil(1.0 / cfg.RequestsPerSecond))

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var allowed bool
			if perKeyLim != nil {
				allowed = perKeyLim.allow(cfg.KeyFunc(r))
			} else {
				allowed = globalBucket.allow()
			}

			if !allowed {
				logger.Warn("rate limit exceeded",
					"path", r.URL.Path,
					"remote", r.RemoteAddr,
					"key", cfg.KeyFunc(r),
				)
				w.Header().Set("X-RateLimit-Limit", itoa(cfg.BurstSize))
				w.Header().Set("X-RateLimit-Remaining", "0")
				w.Header().Set("Retry-After", itoa(retryAfterSecs))
				http.Error(w, "429 Too Many Requests", http.StatusTooManyRequests)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// ============================================================================================
//  helpers 
// ============================================================================================

/* 
Extracts the real client IP from the request, honouring
 X-Forwarded-For set by a trusted upstream load balancer.
 For security in production, restrict this to the first hop only
 if the gateway is directly internet-facing.
*/
func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		// Take the leftmost (original client) IP.
		if idx := indexOf(xff, ','); idx >= 0 {
			return xff[:idx]
		}
		return xff
	}
	// Fall back to RemoteAddr (strip port).
	ip := r.RemoteAddr
	if idx := indexOf(ip, ':'); idx >= 0 {
		ip = ip[:idx]
	}
	return ip
}

func indexOf(s string, c byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == c {
			return i
		}
	}
	return -1
}

/* 
converts an int to its decimal string representation
*/
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	buf := [20]byte{}
	pos := len(buf)
	neg := n < 0
	if neg {
		n = -n
	}
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}
