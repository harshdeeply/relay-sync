import hashlib
import hmac
import json
import sqlite3
import threading
from datetime import datetime, timedelta, timezone


def now():
    return datetime.now(timezone.utc)


def parse_time(value):
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise ValueError("event timestamp must include timezone")
    return dt.astimezone(timezone.utc)


def sign(secret, payload):
    return hmac.new(secret.encode(), payload, hashlib.sha256).hexdigest()


class TemporaryFailure(Exception):
    pass


class Relay:
    def __init__(self, path=":memory:", secret="local-demo-secret", max_attempts=3):
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.lock = threading.RLock()
        self.secret = secret
        self.max_attempts = max_attempts
        self.db.executescript("""
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS inbox (
              event_id TEXT PRIMARY KEY, product_id TEXT NOT NULL, version INTEGER NOT NULL,
              payload TEXT NOT NULL, status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
              next_attempt TEXT NOT NULL, last_error TEXT, received_at TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS source (
              product_id TEXT PRIMARY KEY, version INTEGER NOT NULL, name TEXT NOT NULL,
              price_cents INTEGER NOT NULL, updated_at TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS target (
              product_id TEXT PRIMARY KEY, version INTEGER NOT NULL, name TEXT NOT NULL,
              price_cents INTEGER NOT NULL, updated_at TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS audit (
              id INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL,
              action TEXT NOT NULL, detail TEXT NOT NULL, created_at TEXT NOT NULL);
        """)

    def audit(self, event_id, action, detail=""):
        self.db.execute("INSERT INTO audit(event_id,action,detail,created_at) VALUES(?,?,?,?)",
                        (event_id, action, detail, now().isoformat()))

    def receive(self, raw, signature):
        if len(raw) > 32768 or not hmac.compare_digest(sign(self.secret, raw), signature):
            raise ValueError("invalid signature or oversized payload")
        event = json.loads(raw)
        required = ("event_id", "product_id", "version", "name", "price_cents", "updated_at")
        if any(key not in event for key in required):
            raise ValueError("missing event field")
        if not isinstance(event["version"], int) or isinstance(event["version"], bool) or event["version"] < 1:
            raise ValueError("version must be positive integer")
        if not isinstance(event["price_cents"], int) or isinstance(event["price_cents"], bool) or event["price_cents"] < 0:
            raise ValueError("price_cents must be nonnegative integer")
        if any(not isinstance(event[k], str) or not event[k] or len(event[k]) > 200 for k in ("event_id", "product_id", "name")):
            raise ValueError("invalid event identity or product name")
        parse_time(event["updated_at"])
        with self.lock, self.db:
            existing = self.db.execute("SELECT payload FROM inbox WHERE event_id=?", (event["event_id"],)).fetchone()
            if existing:
                if existing["payload"] != json.dumps(event, sort_keys=True):
                    raise ValueError("event ID reused with different payload")
                return {"accepted": False, "duplicate": True}
            self.db.execute("INSERT INTO inbox(event_id,product_id,version,payload,status,next_attempt,received_at) VALUES(?,?,?,?,?,?,?)",
                            (event["event_id"], event["product_id"], event["version"],
                             json.dumps(event, sort_keys=True), "pending", now().isoformat(), now().isoformat()))
            self.audit(event["event_id"], "received")
        return {"accepted": True, "duplicate": False}

    def _upsert(self, table, event):
        self.db.execute(f"""INSERT INTO {table}(product_id,version,name,price_cents,updated_at) VALUES(?,?,?,?,?)
          ON CONFLICT(product_id) DO UPDATE SET version=excluded.version,name=excluded.name,
          price_cents=excluded.price_cents,updated_at=excluded.updated_at
          WHERE excluded.version > {table}.version""",
                        (event["product_id"], event["version"], event["name"],
                         event["price_cents"], event["updated_at"]))

    def process(self, adapter=None, at=None, limit=100):
        """Process due events. Adapter receives event and may raise TemporaryFailure."""
        at = at or now()
        processed = []
        with self.lock:
            rows = self.db.execute("SELECT * FROM inbox WHERE status='pending' AND next_attempt<=? ORDER BY received_at,event_id LIMIT ?",
                                   (at.isoformat(), limit)).fetchall()
            for row in rows:
                event = json.loads(row["payload"])
                source = self.db.execute("SELECT version FROM source WHERE product_id=?", (event["product_id"],)).fetchone()
                if source and source["version"] >= event["version"]:
                    with self.db:
                        self.db.execute("UPDATE inbox SET status='stale' WHERE event_id=?", (row["event_id"],))
                        self.audit(row["event_id"], "stale", f"source is at v{source['version']}")
                    processed.append((row["event_id"], "stale"))
                    continue
                try:
                    if adapter:
                        adapter(event)
                except TemporaryFailure as exc:
                    attempts = row["attempts"] + 1
                    status = "dead" if attempts >= self.max_attempts else "pending"
                    next_attempt = at + timedelta(seconds=min(60, 2 ** attempts))
                    with self.db:
                        self.db.execute("UPDATE inbox SET attempts=?,status=?,next_attempt=?,last_error=? WHERE event_id=?",
                                        (attempts, status, next_attempt.isoformat(), str(exc)[:200], row["event_id"]))
                        self.audit(row["event_id"], status, str(exc)[:200])
                    processed.append((row["event_id"], status))
                    continue
                with self.db:
                    self._upsert("source", event)
                    self._upsert("target", event)
                    self.db.execute("UPDATE inbox SET status='applied' WHERE event_id=?", (row["event_id"],))
                    self.audit(row["event_id"], "applied", f"v{event['version']}")
                processed.append((row["event_id"], "applied"))
        return processed

    def replay(self, event_id):
        with self.lock, self.db:
            row = self.db.execute("SELECT status FROM inbox WHERE event_id=?", (event_id,)).fetchone()
            if row is None or row["status"] != "dead":
                raise ValueError("only dead events can be replayed")
            self.db.execute("UPDATE inbox SET status='pending',attempts=0,next_attempt=?,last_error=NULL WHERE event_id=?",
                            (now().isoformat(), event_id))
            self.audit(event_id, "replayed")

    def reconcile(self, repair=False):
        """Source snapshot wins. Target edits are flagged, then optionally repaired."""
        with self.lock, self.db:
            drift = []
            for row in self.db.execute("SELECT * FROM source"):
                other = self.db.execute("SELECT * FROM target WHERE product_id=?", (row["product_id"],)).fetchone()
                if other is None or any(row[k] != other[k] for k in ("version", "name", "price_cents")):
                    drift.append({"product_id": row["product_id"], "source_version": row["version"],
                                  "target_version": other["version"] if other else None})
                    if repair:
                        self.db.execute("INSERT INTO target VALUES(?,?,?,?,?) ON CONFLICT(product_id) DO UPDATE SET version=excluded.version,name=excluded.name,price_cents=excluded.price_cents,updated_at=excluded.updated_at",
                                        tuple(row[k] for k in ("product_id", "version", "name", "price_cents", "updated_at")))
                        self.audit(row["product_id"], "reconciled")
            return drift

    def snapshot(self):
        return {table: [dict(row) for row in self.db.execute(f"SELECT * FROM {table} ORDER BY 1")]
                for table in ("source", "target", "inbox", "audit")}
