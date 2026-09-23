import json
import threading
import unittest
from http.server import ThreadingHTTPServer
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from relay.core import Relay, PermanentFailure, TemporaryFailure, sign
from relay.partner import http_adapter
from relay.server import make_handler


class HttpTests(unittest.TestCase):
    def setUp(self):
        self.relay = Relay(secret="webhook-secret")
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(self.relay, "operator-key"))
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.server.server_port}"

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.relay.close()

    def request(self, path, body=None, headers=None, method=None):
        req = Request(self.base + path, body, headers or {}, method=method)
        try:
            with urlopen(req, timeout=3) as response:
                return response.status, json.load(response)
        except HTTPError as exc:
            return exc.code, json.load(exc)

    def test_signed_ingest_authorization_and_deduplication(self):
        body = json.dumps({"event_id":"http-1","product_id":"http-sku","version":1,
                           "name":"Catalog item","price_cents":1200,"updated_at":"2026-09-01T12:00:00Z"}).encode()
        self.assertEqual(self.request("/webhooks/catalog",body,{"X-Relay-Signature":"invalid"})[0],400)
        headers={"X-Relay-Signature":sign("webhook-secret",body)}
        self.assertEqual(self.request("/webhooks/catalog",body,headers)[0],202)
        self.assertEqual(self.request("/webhooks/catalog",body,headers)[0],200)
        self.assertEqual(self.request("/api/metrics",method="GET")[0],401)
        auth={"Authorization":"Bearer operator-key"}
        self.assertEqual(self.request("/api/process",b"",auth)[1]["processed"],[["http-1","applied"]])
        self.assertEqual(self.request("/api/metrics",headers=auth,method="GET")[1]["source_products"],1)

    def test_request_size_and_repair_contract(self):
        auth={"Authorization":"Bearer operator-key"}
        self.assertEqual(self.request("/webhooks/catalog",b"x"*32769)[0],413)
        self.assertEqual(self.request("/api/reconcile",b'{"repair":"yes"}',auth)[0],400)
        body = json.dumps({"event_id":"http-2","product_id":"http-sku","version":1,
                           "name":"Catalog item","price_cents":1200,"updated_at":"2026-09-01T12:00:00Z"}).encode()
        self.request("/webhooks/catalog",body,{"X-Relay-Signature":sign("webhook-secret",body)})
        self.request("/api/process",b"",auth)
        self.relay.db.execute("UPDATE target SET price_cents=5 WHERE product_id='http-sku'")
        self.assertEqual(len(self.request("/api/reconcile",b'{}',auth)[1]["drift"]),1)
        self.assertEqual(len(self.request("/api/reconcile",b'{"repair":true}',auth)[1]["drift"]),1)
        self.assertEqual(self.request("/api/metrics",headers=auth,method="GET")[1]["drifted_products"],0)

    def test_dashboard_demo_walkthrough(self):
        with urlopen(self.base + "/", timeout=3) as response:
            html = response.read().decode()
        self.assertIn("Integration health", html)
        auth={"Authorization":"Bearer operator-key"}
        seeded=self.request("/api/demo/seed",b"{}",auth)
        self.assertEqual(seeded[0],202)
        self.assertEqual(seeded[1]["event"]["product_id"],"SKU-42")
        self.assertEqual(self.request("/api/process",b"{}",auth)[1]["processed"][0][1],"applied")
        self.assertTrue(self.request("/api/demo/drift",b"{}",auth)[1]["mutated"])
        self.assertEqual(self.request("/api/metrics",headers=auth,method="GET")[1]["drifted_products"],1)
        self.request("/api/reconcile",b'{"repair":true}',auth)
        self.assertEqual(self.request("/api/metrics",headers=auth,method="GET")[1]["drifted_products"],0)


class PartnerTests(unittest.TestCase):
    def test_https_and_token_are_required(self):
        with self.assertRaises(ValueError):
            http_adapter("http://example.com/products", "token")
        with self.assertRaises(ValueError):
            http_adapter("https://example.com/products", "")

    def test_adapter_sends_idempotency_key(self):
        seen = []
        class Response:
            status = 204
            def __enter__(self): return self
            def __exit__(self,*args): pass
        class Opener:
            def open(self, request, timeout):
                seen.append((request.get_header("X-idempotency-key"),request.get_header("Authorization"),timeout))
                return Response()
        http_adapter("https://example.com/products", "token", Opener())({"event_id":"event-7"})
        self.assertEqual(seen, [("event-7","Bearer token",10)])

    def test_retryable_and_permanent_http_errors(self):
        from urllib.error import HTTPError
        class Opener:
            def __init__(self, code): self.code=code
            def open(self,request,timeout):
                raise HTTPError(request.full_url,self.code,"error",{},None)
        for code, expected in ((429,TemporaryFailure),(503,TemporaryFailure),(401,PermanentFailure)):
            with self.subTest(code=code), self.assertRaises(expected):
                http_adapter("https://example.com/products","token",Opener(code))({"event_id":"id"})


if __name__ == "__main__":
    unittest.main()
