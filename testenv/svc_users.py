"""
Users service stub — port 9001
"""
from flask import Flask, request, jsonify

app = Flask(__name__)

USERS = {
    "1": {"id": "1", "name": "Alice Nakamura", "email": "alice@example.com"},
    "2": {"id": "2", "name": "Bob Osei",       "email": "bob@example.com"},
    "3": {"id": "3", "name": "Carol Mendes",   "email": "carol@example.com"},
}


@app.get("/api/v1/users")
def list_users():
    return jsonify(list(USERS.values()))


@app.get("/api/v1/users/<uid>")
def get_user(uid):
    user = USERS.get(uid)
    if not user:
        return jsonify({"error": f"user {uid!r} not found"}), 404
    return jsonify(user)


@app.post("/api/v1/users")
def create_user():
    body = request.get_json(force=True)
    new_id = str(len(USERS) + 1)
    body["id"] = new_id
    USERS[new_id] = body
    return jsonify(body), 201


@app.after_request
def tag(resp):
    resp.headers["X-Service"] = "users-stub"
    return resp


if __name__ == "__main__":
    print("Users service listening on :9001")
    app.run(port=9001)
