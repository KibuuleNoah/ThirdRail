#!/usr/bin/env bash
# Starts all three upstream stubs and the ThirdRail gateway.
# Ctrl+C kills everything cleanly.






set -e
cd "../"

# Build the gateway if not already built.
if [ ! -f ./gateway ]; then
  echo "Building gateway..."
  go build -o gateway ./cmd/gateway
fi

# Low thresholds so circuit breaker and retry are easy to trigger manually.
export THIRDRAIL_CB_FAILURE_THRESHOLD=5
export THIRDRAIL_CB_SUCCESS_THRESHOLD=2
export THIRDRAIL_CB_OPEN_TIMEOUT=10s
export THIRDRAIL_MAX_RETRIES=3
export THIRDRAIL_RETRY_BASE_DELAY=100ms
export THIRDRAIL_RETRY_MAX_DELAY=1s
export THIRDRAIL_RATE_RPS=1000
export THIRDRAIL_RATE_BURST=200
export THIRDRAIL_REQUEST_TIMEOUT=20s

pids=()

cleanup() {
  echo ""
  echo "Shutting down..."
  for pid in "${pids[@]}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait
  echo "Done."
}
trap cleanup INT TERM

echo "Starting upstream stubs..."
# python testenv/.venv/bin/activate
python testenv/svc_users.py   &  pids+=($!)
python testenv/svc_orders.py  &  pids+=($!)
python testenv/svc_default.py &  pids+=($!)

sleep 0.5

echo "Starting ThirdRail gateway on :8080..."
./gateway &
pids+=($!)

echo ""
echo "All services running. Press Ctrl+C to stop."
echo ""
echo "  Users service:   http://localhost:9001"
echo "  Orders service:  http://localhost:9002"
echo "  Default service: http://localhost:9000"
echo "  Gateway:         http://localhost:8080"
echo "  Health:          http://localhost:8080/healthz"
echo ""
echo "Run scenarios:"
echo "  python3 testenv/run_scenarios.py"
echo ""

wait
