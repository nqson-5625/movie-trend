"""Stream Module 01 ratings with acknowledged batch checkpoints."""
from __future__ import annotations
import argparse
import csv
import json
import math
import sys
import time
from pathlib import Path

from modules.data_source.src.movie_event_adapter import to_movie_event
from .common import ROOT, STATE, Delivery, Shutdown, atomic_json, bootstrap, digest, locked, utc_now

DEFAULT_RATINGS = ROOT / "data/prepared/movielens/ratings_sorted.csv"


def replay(ratings, checkpoint, servers, rate=100, batch_size=100, limit=0, timeout=30, stop=None, delivery=None):
    if not math.isfinite(rate) or rate <= 0 or batch_size <= 0 or limit < 0:
        raise ValueError("rate/batch-size must be positive and limit non-negative")
    ratings, checkpoint = Path(ratings), Path(checkpoint)
    started = time.monotonic()
    with locked(checkpoint):
        identity = {"version": 1, "sha256": digest(ratings), "bootstrap": servers, "topic": "movie-events"}
        state = json.loads(checkpoint.read_text()) if checkpoint.exists() else {**identity, "next_row": 0}
        if any(state.get(key) != value for key, value in identity.items()):
            raise ValueError("Checkpoint input/target changed; use a new checkpoint for intentional replay")
        cursor = state.get("next_row")
        if type(cursor) is not int or cursor < 0:
            raise ValueError("Invalid checkpoint next_row")
        sender = delivery or Delivery(servers, timeout)
        sent = 0
        try:
            with ratings.open(newline="", encoding="utf-8") as stream:
                reader = csv.DictReader(stream)
                if reader.fieldnames != ["userId", "movieId", "rating", "timestamp"]:
                    raise ValueError("Expected Module 01 ratings_sorted.csv header")
                for _ in range(cursor):
                    if next(reader, None) is None:
                        raise ValueError("Checkpoint exceeds input length")
                while not (stop and stop.requested) and (not limit or sent < limit):
                    rows = []
                    for _ in range(min(batch_size, limit - sent) if limit else batch_size):
                        row = next(reader, None)
                        if row is None:
                            break
                        rows.append(row)
                    if not rows:
                        break
                    # Conversion occurs at enqueue time, retaining original source time.
                    events = (to_movie_event(row, utc_now()) for row in rows)
                    count = sender.send(events, "movie-events", rate, stop)
                    cursor += count
                    sent += count
                    atomic_json(checkpoint, {**identity, "next_row": cursor})
            return {**sender.stats, "next_row": cursor, "elapsed_seconds": time.monotonic() - started,
                    "events_per_second": sent / max(time.monotonic() - started, 0.001),
                    "interrupted": bool(stop and stop.requested)}
        except Exception:
            # Surface partial acknowledgements on failures without advancing checkpoint.
            print(json.dumps({"status": "failed", **sender.stats, "next_row": cursor}), file=sys.stderr)
            raise


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--ratings", type=Path, default=DEFAULT_RATINGS)
    p.add_argument("--checkpoint", type=Path, default=STATE / "movie-checkpoint.json")
    p.add_argument("--bootstrap", default=bootstrap())
    p.add_argument("--rate", type=float, default=100)
    p.add_argument("--batch-size", type=int, default=100)
    p.add_argument("--limit", type=int, default=0)
    p.add_argument("--timeout", type=float, default=30)
    p.add_argument("--report", type=Path)
    args = p.parse_args(argv)
    report = args.report
    del args.report
    try:
        with Shutdown() as stop:
            options = vars(args).copy()
            options["servers"] = options.pop("bootstrap")
            result = replay(**options, stop=stop)
        if report:
            atomic_json(report, result)
        print(json.dumps(result))
        return 130 if result["interrupted"] else 0
    except Exception as exc:
        print(f"MOVIE_PRODUCER_FAIL: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
