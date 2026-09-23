import json
import unittest
from datetime import timedelta
from relay.core import Relay, TemporaryFailure, now, sign


def event(event_id="a", version=1, price=1200):
    return {"event_id": event_id, "product_id": "sku", "version": version,
            "name": "Product", "price_cents": price, "updated_at": "2026-09-01T12:00:00Z"}


class RelayTests(unittest.TestCase):
    def setUp(self):
        self.relay = Relay(max_attempts=2)

    def send(self, value):
        raw = json.dumps(value).encode()
        return self.relay.receive(raw, sign(self.relay.secret, raw))

    def test_signature_and_event_identity(self):
        with self.assertRaises(ValueError):
            self.relay.receive(b"{}", "bad")
        self.assertTrue(self.send(event())["accepted"])
        self.assertTrue(self.send(event())["duplicate"])
        with self.assertRaises(ValueError):
            self.send(event(price=500))

    def test_late_event_and_reconciliation(self):
        self.send(event("new", 2, 2500))
        self.assertEqual(self.relay.process()[0][1], "applied")
        self.send(event("old", 1, 1200))
        self.assertEqual(self.relay.process()[0][1], "stale")
        self.relay.db.execute("UPDATE target SET price_cents=9 WHERE product_id='sku'")
        self.assertEqual(len(self.relay.reconcile()), 1)
        self.relay.reconcile(repair=True)
        self.assertEqual(self.relay.reconcile(), [])
        self.assertEqual(self.relay.snapshot()["target"][0]["price_cents"], 2500)

    def test_dead_letter_and_replay(self):
        self.send(event())
        def down(value):
            raise TemporaryFailure("503")
        self.assertEqual(self.relay.process(adapter=down)[0][1], "pending")
        self.assertEqual(self.relay.process(adapter=down, at=now() + timedelta(hours=1))[0][1], "dead")
        self.relay.replay("a")
        self.assertEqual(self.relay.process()[0][1], "applied")

    def test_timezone_and_amount_validation(self):
        with self.assertRaises(ValueError):
            self.send({**event(), "updated_at": "2026-09-01T12:00:00"})
        with self.assertRaises(ValueError):
            self.send(event(price=-1))


if __name__ == "__main__":
    unittest.main()
