"""Loopback-only operational API for a signed source webhook and manual worker ticks."""
import hmac
import json
import os
from pathlib import Path
from uuid import uuid4
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse

from .core import Relay
from .partner import http_adapter


def make_handler(relay, operator_key, adapter=None):
    class Handler(BaseHTTPRequestHandler):
        server_version = "Relay/1.0"

        def respond(self, status, data):
            body = json.dumps(data, separators=(",", ":")).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def authorized(self):
            expected = "Bearer " + operator_key
            return hmac.compare_digest(self.headers.get("Authorization", ""), expected)

        def do_POST(self):
            path = urlparse(self.path).path
            if path != "/webhooks/catalog" and not self.authorized():
                return self.respond(401, {"error": "operator authorization required"})
            try:
                length = int(self.headers.get("Content-Length", "0"))
                if length < 0 or length > 32768:
                    return self.respond(413, {"error": "payload too large"})
                raw = self.rfile.read(length)
                if path == "/webhooks/catalog":
                    result = relay.receive(raw, self.headers.get("X-Relay-Signature", ""))
                    return self.respond(202 if result["accepted"] else 200, result)
                if path == "/api/process":
                    return self.respond(200, {"processed": relay.process(adapter=adapter)})
                if path == "/api/demo/seed":
                    state = relay.snapshot()
                    version = max([r["version"] for r in state["inbox"] if r["product_id"] == "SKU-42"] + [0]) + 1
                    event = {"event_id": "demo-" + uuid4().hex[:12], "product_id": "SKU-42",
                             "version": version, "name": "Field kit", "price_cents": 1999 + version * 250,
                             "updated_at": "2026-09-23T12:00:00Z"}
                    payload = json.dumps(event).encode()
                    from .core import sign
                    return self.respond(202, {"event": event, **relay.receive(payload, sign(relay.secret, payload))})
                if path == "/api/demo/drift":
                    with relay.lock, relay.db:
                        result = relay.db.execute("UPDATE target SET price_cents=1 WHERE product_id='SKU-42'")
                    return self.respond(200, {"mutated": result.rowcount > 0})
                if path == "/api/reconcile":
                    opts = json.loads(raw or b"{}")
                    if not isinstance(opts, dict) or not isinstance(opts.get("repair", False), bool):
                        raise ValueError("repair must be a boolean")
                    return self.respond(200, {"drift": relay.reconcile(repair=opts.get("repair", False))})
                if path.startswith("/api/replay/"):
                    event_id = path.removeprefix("/api/replay/")
                    relay.replay(event_id)
                    return self.respond(200, {"queued": event_id})
                return self.respond(404, {"error": "unknown route"})
            except (ValueError, UnicodeError, json.JSONDecodeError) as exc:
                return self.respond(400, {"error": str(exc)[:200]})

        def do_GET(self):
            path = urlparse(self.path).path
            if path == "/":
                body = (Path(__file__).parent / "dashboard.html").read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            if not self.authorized():
                return self.respond(401, {"error": "operator authorization required"})
            if path == "/api/metrics":
                return self.respond(200, relay.metrics())
            if path == "/api/state":
                return self.respond(200, relay.snapshot())
            return self.respond(404, {"error": "unknown route"})

        def log_message(self, format, *args):
            pass

    return Handler


def main():
    operator_key = os.environ.get("RELAY_OPERATOR_KEY")
    secret = os.environ.get("RELAY_WEBHOOK_SECRET")
    if not operator_key or not secret:
        raise SystemExit("Set RELAY_OPERATOR_KEY and RELAY_WEBHOOK_SECRET before starting the server")
    url = os.environ.get("RELAY_PARTNER_URL")
    adapter = http_adapter(url, os.environ.get("RELAY_PARTNER_TOKEN")) if url else None
    relay = Relay(os.environ.get("RELAY_DB", "relay.sqlite3"), secret=secret)
    with ThreadingHTTPServer(("127.0.0.1", int(os.environ.get("RELAY_PORT", "8083"))),
                             make_handler(relay, operator_key, adapter)) as server:
        print(f"Relay listening on http://127.0.0.1:{server.server_port}", flush=True)
        server.serve_forever()


if __name__ == "__main__":
    main()
