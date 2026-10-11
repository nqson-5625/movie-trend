"""Fetch once per interval, persist snapshot before publishing, resume on restart."""
from __future__ import annotations
import argparse
import json
import os
import sys
import time
from pathlib import Path

from modules.data_source.src.tmdb_source_adapter import fetch_trending_movies, build_snapshot_records
from .common import STATE, Delivery, Shutdown, atomic_json, bootstrap, locked, utc_now, validate


def publish(state_path, servers, interval=600, retries=3, timeout=30, stop=None, fetch=fetch_trending_movies, delivery=None):
    if interval <= 0 or retries < 0:
        raise ValueError("interval must be positive; retries must be non-negative")
    state_path = Path(state_path)
    with locked(state_path):
        state = json.loads(state_path.read_text()) if state_path.exists() else {
            "version": 1, "bootstrap": servers, "topic": "tmdb-snapshots", "last_attempt": 0,
            "complete": True, "records": [], "next_row": 0,
        }
        if state.get("version") != 1 or state.get("bootstrap") != servers or state.get("topic") != "tmdb-snapshots":
            raise ValueError("TMDB state target/version mismatch")
        if state["complete"]:
            if time.time() - state["last_attempt"] < interval:
                return {"status": "cooldown", "acknowledged": 0}
            key = os.getenv("TMDB_API_KEY", "").strip()
            if not key:
                raise RuntimeError("TMDB_API_KEY is missing in environment/.env")
            for attempt in range(retries + 1):
                if stop and stop.requested:
                    return {"status": "interrupted", "acknowledged": 0}
                state["last_attempt"] = time.time()
                atomic_json(state_path, state)
                try:
                    items, event_time = fetch(key)
                    break
                except RuntimeError as exc:
                    message = str(exc)
                    transient = any(x in message for x in ("429", "HTTP 5", "Cannot connect", "HTTP request failed"))
                    if not transient or attempt == retries:
                        raise
                    delay = max(1, 2 ** attempt)
                    # Conservative wait on rate limiting; adapter does not expose Retry-After.
                    if "429" in message:
                        delay = max(60, delay)
                    deadline = time.monotonic() + delay
                    while time.monotonic() < deadline:
                        if stop and stop.requested:
                            return {"status": "interrupted", "acknowledged": 0}
                        time.sleep(min(0.2, max(0, deadline - time.monotonic())))
            records = build_snapshot_records(items, event_time, utc_now())
            for event in records:
                validate(event, "tmdb-snapshots")
            state.update(records=records, next_row=0, complete=False)
            atomic_json(state_path, state)
        cursor = state["next_row"]
        if type(cursor) is not int or not 0 <= cursor <= len(state["records"]):
            raise ValueError("Invalid snapshot checkpoint")
        sender = delivery or Delivery(servers, timeout)
        # Keep the fetched snapshot/time immutable. Refresh ingest time per enqueue.
        def events():
            for event in state["records"][cursor:]:
                yield {**event, "ingest_time": utc_now()}
        try:
            count = sender.send(events(), "tmdb-snapshots", stop=stop)
        except Exception:
            print(json.dumps({"status": "failed", **sender.stats}), file=sys.stderr)
            raise
        state["next_row"] += count
        state["complete"] = state["next_row"] == len(state["records"])
        atomic_json(state_path, state)
        return {"status": "complete" if state["complete"] else "interrupted", **sender.stats,
                "event_time": state["records"][0]["event_time"], "next_row": state["next_row"]}


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--state", type=Path, default=STATE / "tmdb-state.json")
    p.add_argument("--bootstrap", default=bootstrap())
    p.add_argument("--interval", type=float, default=600)
    p.add_argument("--retries", type=int, default=3)
    p.add_argument("--timeout", type=float, default=30)
    p.add_argument("--loop", action="store_true", help="Schedule snapshots until SIGINT/SIGTERM")
    args = p.parse_args(argv)
    try:
        with Shutdown() as stop:
            while not stop.requested:
                print(json.dumps(publish(args.state, args.bootstrap, args.interval, args.retries, args.timeout, stop)), flush=True)
                if not args.loop:
                    break
                deadline = time.monotonic() + args.interval
                while not stop.requested and time.monotonic() < deadline:
                    time.sleep(0.2)
        return 130 if stop.requested else 0
    except Exception as exc:
        print(f"TMDB_PRODUCER_FAIL: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
