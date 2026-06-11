# ThirdRail

A resilient API gateway in Go. No dependencies outside the standard library, just bare metal & lightwigth.

Handles high-throughput traffic, protects downstream services from cascading failures, and enforces rate limits per client. Built for production: all shared state is race-safe, context cancellation propagates cleanly through the full request lifecycle, and the middleware chain is composable.

## How requests flow

```
Logger → RateLimiter → Timeout → CircuitBreaker → Retry → ReverseProxy
```

Each layer is independent. The circuit breaker wraps the retry so individual retry attempts are each evaluated — a failed probe doesn't silently retry its way to a false recovery.

## Getting started

```bash
git clone https://github.com/KibuuleNoah/ThirdRail
cd ThirdRail
go build ./cmd/gateway
./gateway
```

Starts on `:8080` by default. Configure via environment variables (see below).

## Configuration

All config is via environment variables. Everything has a sane default so the gateway runs out of the box for local dev.

| Variable | Default | Description |
|---|---|---|
| `THIRDRAIL_LISTEN_ADDR` | `:8080` | Address to listen on |
| `THIRDRAIL_REQUEST_TIMEOUT` | `20s` | Default upstream timeout |
| `THIRDRAIL_READ_TIMEOUT` | `10s` | HTTP server read timeout |
| `THIRDRAIL_WRITE_TIMEOUT` | `30s` | HTTP server write timeout |
| `THIRDRAIL_SHUTDOWN_TIMEOUT` | `15s` | Graceful shutdown window |
| `THIRDRAIL_RATE_RPS` | `1000` | Sustained requests/sec per client IP |
| `THIRDRAIL_RATE_BURST` | `200` | Burst capacity above the sustained rate |
| `THIRDRAIL_CB_FAILURE_THRESHOLD` | `5` | Consecutive failures before opening the circuit |
| `THIRDRAIL_CB_SUCCESS_THRESHOLD` | `2` | Consecutive successes in half-open before closing |
| `THIRDRAIL_CB_OPEN_TIMEOUT` | `10s` | How long to stay open before probing |
| `THIRDRAIL_MAX_RETRIES` | `3` | Retries on 502/503/504 |
| `THIRDRAIL_RETRY_BASE_DELAY` | `100ms` | Initial backoff delay |
| `THIRDRAIL_RETRY_MAX_DELAY` | `2s` | Backoff cap |
| `THIRDRAIL_LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |

## Routes

Routes are defined programmatically in `cmd/gateway/main.go`. Longest prefix wins.

```go
cfg.WithRoutes([]config.RouteConfig{
    {
        PathPrefix:  "/api/v1/users",
        UpstreamURL: "http://users-svc:8001",
        StripPrefix: false,
        Timeout:     5 * time.Second, // overrides the global default
    },
    {
        PathPrefix:  "/api",
        UpstreamURL: "http://default-svc:8000",
    },
})
```

Per-route timeouts override `THIRDRAIL_REQUEST_TIMEOUT`. Routes without a timeout use the global default.

## Endpoints

- `GET /healthz` — returns 200 when the circuit is closed, 503 when open. Response body includes the current circuit state as JSON.
- Everything else is proxied based on the routing table.

## Running tests

```bash
go test ./... -race
```

All tests pass with the race detector enabled.

## Project layout

```
cmd/gateway/        entry point, wires the middleware chain
config/             config loading from environment
internal/
  proxy/            router (longest-prefix matching) and reverse proxy handler
  resiliency/       circuit breaker, retry, and timeout middleware
  middleware/       rate limiter and structured logger
```

## Design notes

**Rate limiter** uses a lock-free token bucket per client IP. Tokens are stored as a scaled `atomic.Int64`; the `allow()` path is a CAS loop with no mutex.

**Circuit breaker** uses atomics for state and counters so `Allow()` is lock-free on the common (closed) path. The mutex only acquires during state transitions. Half-open probe slots use `atomic.Bool.CompareAndSwap` to guarantee exactly one probe under concurrent traffic.

**Retry** uses full jitter backoff (`sleep = random(0, cap)`) to avoid synchronized retry waves across gateway instances. Request bodies are buffered up to 1 MiB for replay; larger bodies are proxied once with no retry.

**Context propagation** — the per-route timeout is applied in the proxy Director by overwriting the request struct (`*req = *newReq`), which is the correct way to propagate a new context through `httputil.ReverseProxy`. The cancel function rides in the context and fires on response body close, or in the error handler if the upstream fails.

**Buffer pool** — `httputil.ReverseProxy` is given a `sync.Pool`-backed 32 KiB buffer pool for response body copies, eliminating a significant source of short-lived heap allocations at high throughput.
