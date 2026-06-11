"""
Orders service stub — port 9002

Control endpoints (hit directly, not through the gateway):
    POST /control/flaky        → start returning 503
    POST /control/normal       → back to normal
    POST /control/slow?ms=N    → sleep N ms before every response
    POST /control/reset        → clear all fault state
    GET  /control/status       → show current mode
"""
import time
from flask import Flask, request, jsonify

app = Flask(__name__)

ORDERS = {
    "101": {"id": "101", "user_id": "1", "item": "Keyboard", "total": 129.99},
    "102": {"id": "102", "user_id": "2", "item": "Monitor",  "total": 349.00},
    "103": {"id": "103", "user_id": "1", "item": "USB Hub",  "total": 24.50},
}

state = {"flaky": False, "slow_ms": 0}


#  Control endpoints 

@app.get("/control/status")
def status():
    return jsonify(state)

@app.post("/control/flaky")
def set_flaky():
    state.update(flaky=True)
    return jsonify({"mode": "flaky", "state": state})

@app.post("/control/normal")
def set_normal():
    state.update(flaky=False, slow_ms=0)
    return jsonify({"mode": "normal", "state": state})

@app.post("/control/slow")
def set_slow():
    ms = int(request.args.get("ms", 500))
    state["slow_ms"] = ms
    return jsonify({"mode": f"slow ({ms}ms)", "state": state})

@app.post("/control/reset")
def reset():
    state.update(flaky=False, slow_ms=0)
    return jsonify({"mode": "reset", "state": state})


#  Fault injection 

@app.before_request
def inject_faults():
    if request.path.startswith("/control"):
        return  # never fault the control plane
    if state["slow_ms"] > 0:
        time.sleep(state["slow_ms"] / 1000)
    if state["flaky"]:
        return jsonify({"error": "service temporarily unavailable (flaky mode)"}), 503


#  Normal routes 

@app.get("/api/v1/orders")
def list_orders():
    return jsonify(list(ORDERS.values()))

@app.get("/api/v1/orders/<oid>")
def get_order(oid):
    order = ORDERS.get(oid)
    if not order:
        return jsonify({"error": f"order {oid!r} not found"}), 404
    return jsonify(order)

@app.post("/api/v1/orders")
def create_order():
    body = request.get_json(force=True)
    new_id = str(100 + len(ORDERS) + 1)
    body["id"] = new_id
    ORDERS[new_id] = body
    return jsonify(body), 201

@app.after_request
def tag(resp):
    resp.headers["X-Service"] = "orders-stub"
    return resp


if __name__ == "__main__":
    print("Orders service listening on :9002")
    print("  POST /control/flaky       → start returning 503")
    print("  POST /control/normal      → back to normal")
    print("  POST /control/slow?ms=N   → add Nms latency")
    print("  POST /control/reset       → clear all faults")
    app.run(port=9002)
