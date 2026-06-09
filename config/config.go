// Package config handles all configuration loading and validation for ThirdRail.
// Config is loaded once at startup from environment variables and/or a YAML-style
// map, then treated as immutable to avoid any hot-path locking overhead.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// RouteConfig defines a single upstream routing rule.
// PathPrefix is matched greedily; the first match wins.
type RouteConfig struct {
	// PathPrefix is the URL prefix to match (e.g. "/api/v1/users").
	PathPrefix string

	// UpstreamURL is the base URL of the target service (e.g. "http://users-svc:8080").
	UpstreamURL string

	// StripPrefix, when true, removes PathPrefix before forwarding the request.
	StripPrefix bool

	// Timeout overrides the global RequestTimeout for this specific route.
	// Zero means use the global default.
	Timeout time.Duration
}

// ResiliencyConfig controls circuit breaker and retry behaviour.
type ResiliencyConfig struct {
	// CircuitBreaker settings
	CBFailureThreshold uint32        // consecutive failures before opening
	CBSuccessThreshold uint32        // consecutive successes in half-open before closing
	CBOpenTimeout      time.Duration // how long to stay Open before moving to Half-Open

	// Retry settings
	MaxRetries      int
	RetryBaseDelay  time.Duration
	RetryMaxDelay   time.Duration
	RetryMultiplier float64 // exponential backoff multiplier
}

// RateLimitConfig controls per-route or global token-bucket parameters.
type RateLimitConfig struct {
	// RequestsPerSecond is the sustained fill rate of the token bucket.
	RequestsPerSecond float64

	// BurstSize is the maximum number of tokens the bucket can hold.
	// Allows short bursts above the sustained rate.
	BurstSize int
}

// ServerConfig holds HTTP server tuning parameters.
type ServerConfig struct {
	ListenAddr      string
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	IdleTimeout     time.Duration
	ShutdownTimeout time.Duration
}

// Config is the root configuration object. It is constructed once and
// passed as a pointer throughout the application. After Init() returns
// it must be treated as read-only.
type Config struct {
	Server     ServerConfig
	Routes     []RouteConfig
	Resiliency ResiliencyConfig
	RateLimit  RateLimitConfig

	// RequestTimeout is the default end-to-end deadline for proxied requests.
	RequestTimeout time.Duration

	// LogLevel controls verbosity: "debug", "info", "warn", "error".
	LogLevel string
}

// Load builds a Config from environment variables, applying sane defaults
// for any value that is not explicitly set. This keeps the gateway operational
// out-of-the-box for development without requiring a config file.
//
// Environment variable reference:
//
//	THIRDRAIL_LISTEN_ADDR          (default ":8080")
//	THIRDRAIL_READ_TIMEOUT         (default "10s")
//	THIRDRAIL_WRITE_TIMEOUT        (default "30s")
//	THIRDRAIL_IDLE_TIMEOUT         (default "60s")
//	THIRDRAIL_SHUTDOWN_TIMEOUT     (default "15s")
//	THIRDRAIL_REQUEST_TIMEOUT      (default "20s")
//	THIRDRAIL_LOG_LEVEL            (default "info")
//
//	THIRDRAIL_CB_FAILURE_THRESHOLD (default "5")
//	THIRDRAIL_CB_SUCCESS_THRESHOLD (default "2")
//	THIRDRAIL_CB_OPEN_TIMEOUT      (default "10s")
//
//	THIRDRAIL_MAX_RETRIES          (default "3")
//	THIRDRAIL_RETRY_BASE_DELAY     (default "100ms")
//	THIRDRAIL_RETRY_MAX_DELAY      (default "2s")
//	THIRDRAIL_RETRY_MULTIPLIER     (default "2.0")
//
//	THIRDRAIL_RATE_RPS             (default "1000")
//	THIRDRAIL_RATE_BURST           (default "200")
//
// Routes are provided programmatically via WithRoutes() for now; YAML
// support is a planned extension.
func Load() (*Config, error) {
	cfg := &Config{}

	cfg.Server = ServerConfig{
		ListenAddr:      envStr("THIRDRAIL_LISTEN_ADDR", ":8080"),
		ReadTimeout:     envDuration("THIRDRAIL_READ_TIMEOUT", 10*time.Second),
		WriteTimeout:    envDuration("THIRDRAIL_WRITE_TIMEOUT", 30*time.Second),
		IdleTimeout:     envDuration("THIRDRAIL_IDLE_TIMEOUT", 60*time.Second),
		ShutdownTimeout: envDuration("THIRDRAIL_SHUTDOWN_TIMEOUT", 15*time.Second),
	}

	cfg.RequestTimeout = envDuration("THIRDRAIL_REQUEST_TIMEOUT", 20*time.Second)
	cfg.LogLevel = envStr("THIRDRAIL_LOG_LEVEL", "info")

	cfg.Resiliency = ResiliencyConfig{
		CBFailureThreshold: uint32(envInt("THIRDRAIL_CB_FAILURE_THRESHOLD", 5)),
		CBSuccessThreshold: uint32(envInt("THIRDRAIL_CB_SUCCESS_THRESHOLD", 2)),
		CBOpenTimeout:      envDuration("THIRDRAIL_CB_OPEN_TIMEOUT", 10*time.Second),
		MaxRetries:         envInt("THIRDRAIL_MAX_RETRIES", 3),
		RetryBaseDelay:     envDuration("THIRDRAIL_RETRY_BASE_DELAY", 100*time.Millisecond),
		RetryMaxDelay:      envDuration("THIRDRAIL_RETRY_MAX_DELAY", 2*time.Second),
		RetryMultiplier:    envFloat("THIRDRAIL_RETRY_MULTIPLIER", 2.0),
	}

	cfg.RateLimit = RateLimitConfig{
		RequestsPerSecond: envFloat("THIRDRAIL_RATE_RPS", 1000),
		BurstSize:         envInt("THIRDRAIL_RATE_BURST", 200),
	}

	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	return cfg, nil
}

// WithRoutes is a functional option that injects a routing table into an
// existing Config. Useful in tests and for programmatic configuration.
func (c *Config) WithRoutes(routes []RouteConfig) *Config {
	c.Routes = routes
	return c
}

// validate performs basic sanity checks to catch misconfigurations at startup.
func (c *Config) validate() error {
	if c.Server.ListenAddr == "" {
		return fmt.Errorf("THIRDRAIL_LISTEN_ADDR must not be empty")
	}
	if c.Resiliency.MaxRetries < 0 {
		return fmt.Errorf("THIRDRAIL_MAX_RETRIES must be >= 0")
	}
	if c.RateLimit.RequestsPerSecond <= 0 {
		return fmt.Errorf("THIRDRAIL_RATE_RPS must be > 0")
	}
	if c.RateLimit.BurstSize <= 0 {
		return fmt.Errorf("THIRDRAIL_RATE_BURST must be > 0")
	}
	seen := make(map[string]struct{}, len(c.Routes))
	for i, r := range c.Routes {
		if r.PathPrefix == "" {
			return fmt.Errorf("route[%d]: PathPrefix must not be empty", i)
		}
		if r.UpstreamURL == "" {
			return fmt.Errorf("route[%d]: UpstreamURL must not be empty", i)
		}
		if _, dup := seen[r.PathPrefix]; dup {
			return fmt.Errorf("route[%d]: duplicate PathPrefix %q", i, r.PathPrefix)
		}
		seen[r.PathPrefix] = struct{}{}
	}
	return nil
}

// ─── helpers ──────────────────────────────────────────────────────────────────

func envStr(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}

func envInt(key string, def int) int {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func envFloat(key string, def float64) float64 {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return def
	}
	return f
}
