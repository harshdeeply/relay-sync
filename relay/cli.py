import argparse
import json
import os
import sys

from .core import Relay, sign


def main():
    parser = argparse.ArgumentParser(description="Run a resilient product catalog integration")
    parser.add_argument("--db", default="relay.sqlite3")
    sub = parser.add_subparsers(dest="command", required=True)
    intake = sub.add_parser("receive")
    intake.add_argument("file", help="UTF-8 JSON event; for the demo the CLI signs it locally")
    sub.add_parser("process")
    sub.add_parser("inspect")
    reconcile = sub.add_parser("reconcile")
    reconcile.add_argument("--repair", action="store_true")
    replay = sub.add_parser("replay")
    replay.add_argument("event_id")
    args = parser.parse_args()
    relay = Relay(args.db, secret=os.environ.get("RELAY_WEBHOOK_SECRET", "local-demo-secret"))
    if args.command == "receive":
        with open(args.file, "rb") as f:
            raw = f.read()
        result = relay.receive(raw, sign(relay.secret, raw))
    elif args.command == "process":
        result = relay.process()
    elif args.command == "reconcile":
        result = relay.reconcile(repair=args.repair)
    elif args.command == "replay":
        relay.replay(args.event_id)
        result = {"queued": args.event_id}
    else:
        result = relay.snapshot()
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError) as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(1)
