import json
import tempfile
import unittest
from datetime import timedelta
from relay.core import Relay, PermanentFailure, TemporaryFailure, now, sign


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

    def test_equal_version_conflict_is_quarantined(self):
        self.send(event("first", 1))
        self.relay.process()
        self.send(event("second", 1, 1500))
        self.assertEqual(self.relay.process(), [("second", "dead")])
        self.assertEqual(self.relay.snapshot()["target"][0]["price_cents"], 1200)

    def test_equal_version_replay_with_new_event_id_is_stale(self):
        self.send(event("first", 1))
        self.relay.process()
        self.send(event("second", 1))
        self.assertEqual(self.relay.process(), [("second", "stale")])

    def test_permanent_partner_failure_dead_letters_without_retry(self):
        self.send(event())
        def rejected(value):
            raise PermanentFailure("invalid destination")
        self.assertEqual(self.relay.process(adapter=rejected), [("a", "dead")])
        self.assertEqual(self.relay.metrics()["events"]["dead"], 1)

    def test_restart_preserves_pending_and_applied_state(self):
        with tempfile.TemporaryDirectory() as temp:
            path = temp + "/relay.sqlite3"
            first = Relay(path)
            raw = json.dumps(event()).encode()
            first.receive(raw, sign(first.secret, raw))
            first.close()
            second = Relay(path)
            self.assertEqual(second.process(), [("a", "applied")])
            second.close()
            third = Relay(path)
            self.assertTrue(third.receive(raw, sign(third.secret, raw))["duplicate"])
            self.assertEqual(third.snapshot()["target"][0]["price_cents"], 1200)
            third.close()

    def test_invalid_envelope_and_limits(self):
        for payload in ([], {**event(), "extra": "secret"}, {**event(), "updated_at": 3}):
            with self.subTest(payload=payload), self.assertRaises(ValueError):
                self.send(payload)
        with self.assertRaises(ValueError):
            self.relay.process(limit=0)
        with self.assertRaises(ValueError):
            Relay(max_attempts=0)

    def test_metrics_report_pending_age_and_drift(self):
        self.send(event())
        self.assertEqual(self.relay.metrics()["events"]["pending"], 1)
        self.assertIsNotNone(self.relay.metrics()["oldest_pending_at"])
        self.relay.process()
        self.relay.db.execute("UPDATE target SET price_cents=99")
        self.assertEqual(self.relay.metrics()["drifted_products"], 1)


if __name__ == "__main__":
    unittest.main()
