"""A reproducible customer incident: duplicate, stale, target drift, and retry."""
import json
from datetime import timedelta

from .core import Relay, TemporaryFailure, now, sign


def send(relay, event):
    raw = json.dumps(event).encode()
    return relay.receive(raw, sign(relay.secret, raw))


def run():
    relay = Relay()
    base = {"product_id": "SKU-42", "name": "Rental welcome kit", "price_cents": 1999,
            "updated_at": "2026-09-01T12:00:00Z"}
    v2 = {**base, "event_id": "e-2", "version": 2, "price_cents": 2499}
    v1 = {**base, "event_id": "e-1", "version": 1}
    events = []
    events.append({"webhook": send(relay, v2)})
    events.append({"webhook_duplicate": send(relay, v2)})
    events.append({"processed": relay.process()})
    events.append({"late_webhook": send(relay, v1)})
    events.append({"processed": relay.process()})
    relay.db.execute("UPDATE target SET price_cents=1 WHERE product_id='SKU-42'")
    events.append({"drift": relay.reconcile()})
    events.append({"repaired": relay.reconcile(repair=True)})
    v3 = {**base, "event_id": "e-3", "version": 3, "price_cents": 2999}
    send(relay, v3)
    failing = lambda event: (_ for _ in ()).throw(TemporaryFailure("partner API 503"))
    for step in range(3):
        events.append({"retry": relay.process(adapter=failing, at=now() + timedelta(hours=step + 1))})
    relay.replay("e-3")
    events.append({"after_replay": relay.process()})
    return {"timeline": events, "final_target": relay.snapshot()["target"],
            "audit": relay.snapshot()["audit"]}


if __name__ == "__main__":
    print(json.dumps(run(), indent=2))
