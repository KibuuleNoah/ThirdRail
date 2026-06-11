# testenv

Live integration test harness for ThirdRail. Spins up three real upstream
services using Flask, then fires scenarios through the gateway to verify every
middleware layer under actual HTTP traffic.

No mocks. No test doubles inside the gateway itself. The gateway runs exactly
as it would in production.

## Dependencies

```
pip install flask requests
```

Python 3.10+ required

## Files

```
testenv/
├── start.sh          # builds the gateway, starts all four processes
├── svc_users.py      # users service stub     → :9001
├── svc_orders.py     # orders service stub    → :9002  (+ fault injection API)
├── svc_default.py    # catch-all echo stub    → :9000
└── run_scenarios.py  # scenario runner        → talks to :8080
```

## Quickstart

```bash
# Terminal 1 — start everything
bash testenv/start.sh

# Terminal 2 — run all scenarios
python3 testenv/run_scenarios.py
```

`start.sh` can be called from anywhere in the repo

## Stubs

### svc_users.py — port 9001

Stateful in-memory CRUD service. Starts with three seeded users.

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/users` | List all users |
| `GET` | `/api/v1/users/<id>` | Get one user; 404 if not found |
| `POST` | `/api/v1/users` | Create user; returns 201 with assigned `id` |

All responses include `X-Service: users-stub`.

---

### svc_orders.py — port 9002

Orders CRUD service with a runtime fault-injection control plane. The control
endpoints are on a separate path prefix and are never affected by the fault
state themselves.

**Order routes**

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/orders` | List all orders |
| `GET` | `/api/v1/orders/<id>` | Get one order; 404 if not found |
| `POST` | `/api/v1/orders` | Create order; returns 201 |

**Control plane** (call directly, not through the gateway)

| Method | Path | Effect |
|--------|------|--------|
| `POST` | `/control/flaky` | Every subsequent order request returns 503 |
| `POST` | `/control/normal` | Clears flaky mode and slow mode |
| `POST` | `/control/slow?ms=N` | Sleeps N milliseconds before every response |
| `POST` | `/control/reset` | Clears all fault state |
| `GET` | `/control/status` | Returns current `{"flaky": bool, "slow_ms": int}` |

Fault injection is implemented as a `@app.before_request` hook, so it applies
uniformly to every order route without touching the route handlers.

All responses include `X-Service: orders-stub`.

---

### svc_default.py — port 9000

Catch-all echo server. Matches every method and path, returns a JSON object
reflecting exactly what the gateway forwarded.

```json
{
  "service": "default-stub",
  "method": "GET",
  "path": "/api/anything",
  "headers": {
    "x-forwarded-for": "127.0.0.1",
    "x-forwarded-host": "localhost:8080",
    "x-forwarded-proto": "http",
    "x-gateway": "ThirdRail",
    "user-agent": "python-requests/2.31.0"
  },
  "body": null
}
```

Used to verify that the gateway sets the correct forwarding headers on every
proxied request.

## Scenario runner

`run_scenarios.py` uses a persistent `requests.Session` for all gateway calls
and drives the orders control plane to set up each fault condition before
testing the gateway's response to it.

### Scenarios

| Scenario | What it tests |
|----------|---------------|
| **Health check** | `GET /healthz` returns 200 and correct circuit state |
| **Routing** | Longest-prefix matching routes to the right stub; unmatched paths get 404 from the gateway |
| **Proxy headers** | `X-Forwarded-For`, `X-Forwarded-Host`, and `X-Gateway` are set on every forwarded request |
| **POST body forwarding** | Request body is preserved and forwarded intact |
| **Retry** | Orders stub goes flaky, then recovers after 350ms; gateway retries and returns 200 |
| **Circuit breaker** | 6 consecutive 503s open the circuit; gateway returns 503 without hitting the upstream; after `CB_OPEN_TIMEOUT` the probe succeeds and the circuit closes |
| **Timeout** | Stub sleeps 12s against a 10s route timeout; gateway returns 504 in ~10s |
| **Rate limiting** | 250 concurrent requests with `burst=200`; verifies both 200s and 429s are returned |

### Execution order

Retry runs before circuit breaker intentionally. The retry scenario causes a few
upstream failures, and the circuit breaker scenario requires starting from a
clean closed state. Running retry first keeps the failure count below the
trip threshold when the circuit breaker test begins.

### Concurrency

The rate-limit scenario uses `ThreadPoolExecutor(max_workers=50)` to fire 250
requests concurrently. `pool.map` collects all status codes, which are then
counted to assert both that the burst was partially served and that the excess
was rejected with 429.

### Output

```
── Circuit breaker — open / probe / close ───────────────
  → orders flaky, sending 6 requests to trip breaker (threshold=5)
  ✓  Breaker open → 503 from gateway (no upstream hit)  status=503
  ✓  /healthz reports Open  status=503  state=Open
  → stub restored, waiting 11s for open timeout...
  ✓  Probe succeeds → 200 after timeout  status=200
  ✓  /healthz reports Closed after recovery  status=200  state=Closed

───────────────────────────────────────────────────────
  16/16 passed
```

Exit code is `0` on full pass, `1` if any check fails.

## Running stubs individually

Each stub can be started on its own for manual exploration:

```bash
python3 testenv/svc_users.py    # :9001
python3 testenv/svc_orders.py   # :9002
python3 testenv/svc_default.py  # :9000
```

Manually trigger fault modes while the gateway is running:

```bash
# Make orders flaky
curl -X POST http://localhost:9002/control/flaky

# Watch the circuit breaker open (fire 6 requests through the gateway)
for i in $(seq 6); do curl -s -o /dev/null -w "%{http_code}\n" http://localhost:8080/api/v1/orders; done

# Restore orders
curl -X POST http://localhost:9002/control/normal

# Check circuit state
curl http://localhost:8080/healthz
```

## Environment variables (set by start.sh)

| Variable | Value in testenv | Description |
|----------|-----------------|-------------|
| `THIRDRAIL_CB_FAILURE_THRESHOLD` | `5` | Failures before circuit opens |
| `THIRDRAIL_CB_SUCCESS_THRESHOLD` | `2` | Probe successes before circuit closes |
| `THIRDRAIL_CB_OPEN_TIMEOUT` | `10s` | Time in open state before probe |
| `THIRDRAIL_MAX_RETRIES` | `3` | Max retry attempts on 502/503/504 |
| `THIRDRAIL_RETRY_BASE_DELAY` | `100ms` | Initial backoff |
| `THIRDRAIL_RETRY_MAX_DELAY` | `1s` | Backoff cap |
| `THIRDRAIL_RATE_RPS` | `1000` | Sustained requests/sec per IP |
| `THIRDRAIL_RATE_BURST` | `200` | Burst capacity |
| `THIRDRAIL_REQUEST_TIMEOUT` | `20s` | Global request timeout |
