"""
ThirdRail scenario runner

Exercises each gateway layer against the live Python stubs.
Start everything first, then:

    python testenv/run_scenarios.py
"""
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

GATEWAY     = "http://localhost:8080"
ORDERS_CTRL = "http://localhost:9002"

GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

passed = 0
failed = 0

session = requests.Session()


#  Helpers 

def ctrl(path, method="POST", **kwargs):
    """Send a control command directly to the orders stub."""
    session.request(method, f"{ORDERS_CTRL}{path}", **kwargs)


def check(name, *, got, want=None, condition=True, note=""):
    global passed, failed
    status_ok = (want is None) or (got == want)
    ok = status_ok and condition
    icon  = f"{GREEN}✓{RESET}" if ok else f"{RED}✗{RESET}"
    label = f"{BOLD}{name}{RESET}"
    extra = f"  status={got}" + (f"  {note}" if note else "")
    print(f"  {icon}  {label}{extra}")
    if not ok:
        failed += 1
        if not status_ok:
            print(f"     {RED}expected {want}, got {got}{RESET}")
    else:
        passed += 1


def section(title):
    bar = "" * (52 - len(title))
    print(f"\n{BOLD}{YELLOW} {title} {bar}{RESET}")


#  Scenarios 

def test_health():
    section("Health check")
    r = session.get(f"{GATEWAY}/healthz")
    check("GET /healthz", got=r.status_code, want=200,
          note=f"circuit={r.json().get('status')}")


def test_routing():
    section("Routing — longest-prefix matching")

    r = session.get(f"{GATEWAY}/api/v1/users")
    check("GET /api/v1/users → users stub", got=r.status_code, want=200,
          note=f"X-Service={r.headers.get('X-Service')}")

    r = session.get(f"{GATEWAY}/api/v1/users/1")
    check("GET /api/v1/users/1 → correct user", got=r.status_code, want=200,
          condition=r.json().get("name") == "Alice Nakamura",
          note=f"name={r.json().get('name')}")

    r = session.get(f"{GATEWAY}/api/v1/users/999")
    check("GET /api/v1/users/999 → 404 from upstream", got=r.status_code, want=404)

    r = session.get(f"{GATEWAY}/api/v1/orders")
    check("GET /api/v1/orders → orders stub", got=r.status_code, want=200,
          note=f"X-Service={r.headers.get('X-Service')}")

    r = session.get(f"{GATEWAY}/api/anything")
    check("GET /api/anything → default stub", got=r.status_code, want=200,
          condition=r.json().get("service") == "default-stub",
          note=f"service={r.json().get('service')}")

    r = session.get(f"{GATEWAY}/notaroute")
    check("GET /notaroute → 404 from gateway", got=r.status_code, want=404)


def test_proxy_headers():
    section("Proxy headers forwarded by gateway")
    r = session.get(f"{GATEWAY}/api/echo")
    hdrs = r.json().get("headers", {})
    check("X-Forwarded-For set",   got=r.status_code, want=200,
          condition=hdrs.get("x-forwarded-for") is not None)
    check("X-Forwarded-Host set",  got=r.status_code, want=200,
          condition=hdrs.get("x-forwarded-host") is not None)
    check("X-Gateway: ThirdRail",  got=r.status_code, want=200,
          condition=hdrs.get("x-gateway") == "ThirdRail")


def test_post():
    section("POST — body forwarding")
    payload = {"name": "Dave Kowalski", "email": "dave@example.com"}
    r = session.post(f"{GATEWAY}/api/v1/users", json=payload)
    check("POST /api/v1/users creates user", got=r.status_code, want=201,
          condition=r.json().get("email") == "dave@example.com",
          note=f"id={r.json().get('id')}")


def test_retry():
    section("Retry — transient upstream failures")
    ctrl("/control/reset")
    time.sleep(0.1)

    # Turn orders flaky, then restore after 350ms.
    # The gateway will retry with jitter backoff; at least one attempt
    # should land on the recovered stub.
    ctrl("/control/flaky")
    threading.Timer(0.35, lambda: ctrl("/control/normal")).start()

    print(f"  {YELLOW}→ orders flaky for ~350ms, gateway should retry to 200{RESET}")
    r = session.get(f"{GATEWAY}/api/v1/orders", timeout=15)
    check("Gateway retries → eventually 200", got=r.status_code, want=200,
          note="stub restored mid-retry window")

    ctrl("/control/reset")


def test_circuit_breaker():
    section("Circuit breaker — open / probe / close")
    ctrl("/control/flaky")
    time.sleep(0.1)

    print(f"  {YELLOW}→ orders flaky, sending 6 requests to trip breaker (threshold=5){RESET}")
    for _ in range(6):
        session.get(f"{GATEWAY}/api/v1/orders", timeout=5)

    r = session.get(f"{GATEWAY}/api/v1/orders", timeout=5)
    check("Breaker open → 503 from gateway (no upstream hit)", got=r.status_code, want=503)

    r = session.get(f"{GATEWAY}/healthz")
    check("/healthz reports Open", got=r.status_code, want=503,
          condition=r.json().get("status") == "Open",
          note=f"state={r.json().get('status')}")

    ctrl("/control/normal")
    print(f"  {YELLOW}→ stub restored, waiting 11s for open timeout...{RESET}")
    time.sleep(11)

    r = session.get(f"{GATEWAY}/api/v1/orders", timeout=5)
    check("Probe succeeds → 200 after timeout", got=r.status_code, want=200)

    r = session.get(f"{GATEWAY}/healthz")
    check("/healthz reports Closed after recovery", got=r.status_code, want=200,
          condition=r.json().get("status") == "Closed",
          note=f"state={r.json().get('status')}")


def test_timeout():
    section("Timeout — route-level deadline enforced")
    ctrl("/control/slow", params={"ms": 12000})
    time.sleep(0.1)

    print(f"  {YELLOW}→ orders sleeping 12s, route timeout is 10s{RESET}")
    start = time.time()
    r = session.get(f"{GATEWAY}/api/v1/orders/101", timeout=15)
    elapsed = time.time() - start

    check("Slow upstream → 504 Gateway Timeout", got=r.status_code, want=504,
          note=f"elapsed={elapsed:.2f}s")
    check("Timeout fired before 12s elapsed", got=200, want=200,
          condition=elapsed < 11.5,
          note=f"elapsed={elapsed:.2f}s")

    ctrl("/control/reset")


def test_rate_limit():
    section("Rate limiting — token bucket per IP")
    print(f"  {YELLOW}→ firing 250 concurrent requests (burst=200){RESET}")

    def fire(_):
        return session.get(f"{GATEWAY}/api/v1/users", timeout=5).status_code

    with ThreadPoolExecutor(max_workers=50) as pool:
        codes = list(pool.map(fire, range(250)))

    n_200 = codes.count(200)
    n_429 = codes.count(429)
    print(f"  {YELLOW}  200={n_200}  429={n_429}{RESET}")

    check("Some requests allowed",  got=200, want=200, condition=n_200 > 0)
    check("Some requests rejected", got=200, want=200, condition=n_429 > 0,
          note=f"429s={n_429}/250")


#  Entry point 

def main():
    print(f"\n{BOLD}ThirdRail scenario runner{RESET}")
    print(f"Gateway: {GATEWAY}\n")

    try:
        requests.get(f"{GATEWAY}/healthz", timeout=2).raise_for_status()
    except Exception:
        print(f"{RED}Cannot reach gateway at {GATEWAY}{RESET}")
        print("Start everything first:")
        print("  bash testenv/start.sh")
        sys.exit(1)

    test_health()
    test_routing()
    test_proxy_headers()
    test_post()
    test_retry()          # before circuit breaker to keep failure count low
    test_circuit_breaker()
    test_timeout()
    test_rate_limit()

    total = passed + failed
    color = GREEN if failed == 0 else RED
    print(f"\n{''*55}")
    print(f"  {color}{BOLD}{passed}/{total} passed{RESET}" +
          (f"  {RED}({failed} failed){RESET}" if failed else ""))
    print()
    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
