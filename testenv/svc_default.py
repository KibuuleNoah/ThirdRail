"""
Default catch-all stub — port 9000

Echoes back the request so you can inspect what the gateway forwarded:
headers, path, method, body.
"""
from flask import Flask, request, jsonify

app = Flask(__name__)


@app.route("/", defaults={"path": ""}, methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
@app.route("/<path:path>",             methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
def echo(path):
    return jsonify({
        "service": "default-stub",
        "method":  request.method,
        "path":    request.full_path.rstrip("?"),
        "headers": {
            "x-forwarded-for":   request.headers.get("X-Forwarded-For"),
            "x-forwarded-host":  request.headers.get("X-Forwarded-Host"),
            "x-forwarded-proto": request.headers.get("X-Forwarded-Proto"),
            "x-gateway":         request.headers.get("X-Gateway"),
            "user-agent":        request.headers.get("User-Agent"),
        },
        "body": request.get_data(as_text=True) or None,
    })


@app.after_request
def tag(resp):
    resp.headers["X-Service"] = "default-stub"
    return resp


if __name__ == "__main__":
    print("Default service listening on :9000")
    app.run(port=9000)
