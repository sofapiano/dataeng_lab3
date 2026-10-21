"""Проверка доступности консоли RustFS (порт 9001) и её API логина."""
import json
import urllib.request
import urllib.error

BASE = "http://localhost:9001"


def get(path):
    req = urllib.request.Request(BASE + path, method="GET")
    req.add_header("Accept", "*/*")
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            body = r.read(400)
            return r.status, r.headers.get("content-type"), body
    except urllib.error.HTTPError as e:
        return e.code, e.headers.get("content-type"), e.read(400)
    except Exception as e:  # noqa: BLE001
        return None, type(e).__name__, str(e)[:200]


for p in ["/", "/index.html", "/login", "/api/v1/login", "/api/v1/health", "/health"]:
    st, ct, body = get(p)
    print(f"GET {p:20s} -> {st} | {ct} | {body[:160]!r}")

# POST login with the known credentials
payload = json.dumps({"accessKey": "rustfsadmin", "secretKey": "rustfsadmin"}).encode()
req = urllib.request.Request(BASE + "/api/v1/login", data=payload, method="POST")
req.add_header("Content-Type", "application/json")
try:
    with urllib.request.urlopen(req, timeout=10) as r:
        print("POST /api/v1/login ->", r.status, r.read(400))
except urllib.error.HTTPError as e:
    print("POST /api/v1/login ->", e.code, e.read(400))
except Exception as e:  # noqa: BLE001
    print("POST /api/v1/login ->", type(e).__name__, str(e)[:200])
