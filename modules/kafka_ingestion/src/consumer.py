"""Validate both topics; durable audit/dedup precedes synchronous offset commit."""
from __future__ import annotations
import argparse
import base64
import hashlib
import json
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Consumer, KafkaError
from jsonschema import ValidationError
from .common import STATE, Shutdown, atomic_json, bootstrap, decode, locked, validate


class Audit:
    def __init__(self, path, identity):
        self.db = sqlite3.connect(path)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript('''
            CREATE TABLE IF NOT EXISTS metadata (identity TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS events (topic TEXT, event_id TEXT, fingerprint TEXT,
                PRIMARY KEY(topic, event_id));
            CREATE TABLE IF NOT EXISTS messages (topic TEXT, partition_id INTEGER, offset_id INTEGER,
                status TEXT, reason TEXT, payload TEXT, PRIMARY KEY(topic, partition_id, offset_id));
        ''')
        row = self.db.execute("SELECT identity FROM metadata").fetchone()
        if row and row[0] != identity:
            self.db.close()
            raise ValueError("Audit belongs to another broker/group; use a separate database")
        if not row:
            self.db.execute("INSERT INTO metadata VALUES (?)", (identity,))
            self.db.commit()

    def record(self, message):
        topic, partition, offset = message.topic(), message.partition(), message.offset()
        raw = message.value()
        latency = None
        try:
            event = decode(raw)
            if message.key() is None:
                raise ValueError("Missing Kafka key")
            validate(event, topic, message.key())
            latency = (datetime.now(timezone.utc) - datetime.fromisoformat(event["ingest_time"].replace("Z", "+00:00"))).total_seconds()
            fingerprint = hashlib.sha256(json.dumps({k: v for k, v in event.items() if k != "ingest_time"}, sort_keys=True).encode()).hexdigest()
            existing = self.db.execute("SELECT fingerprint FROM events WHERE topic=? AND event_id=?", (topic, event["event_id"])).fetchone()
            status = "valid" if not existing else ("duplicate" if existing[0] == fingerprint else "conflict")
            reason = "" if status != "conflict" else "Same event_id with changed source payload"
        except (ValueError, TypeError, UnicodeError, ValidationError) as exc:
            event = None
            status, reason = "invalid", str(exc).splitlines()[0][:500]
        with self.db:
            if event is not None:
                self.db.execute("INSERT OR IGNORE INTO events VALUES (?,?,?)", (topic, event["event_id"], fingerprint))
            self.db.execute("INSERT OR IGNORE INTO messages VALUES (?,?,?,?,?,?)", (
                topic, partition, offset, status, reason,
                base64.b64encode(raw).decode() if raw is not None else None,
            ))
        return status, latency

    def close(self):
        self.db.close()


def consume(servers, group, audit_path, limit=0, idle_timeout=10, stop=None, report=None):
    if limit < 0 or idle_timeout <= 0:
        raise ValueError("limit must be non-negative; idle-timeout must be positive")
    audit_path = Path(audit_path)
    stats = {"received": 0, "valid": 0, "invalid": 0, "duplicate": 0, "conflict": 0, "offset_regressions": 0,
             "offset_gaps": 0, "latency_seconds_mean": None, "latency_seconds_max": None, "lag": {}}
    started = time.monotonic()
    last = {}
    latency_sum = 0
    latency_count = 0
    with locked(audit_path):
        audit = Audit(audit_path, json.dumps([servers, group]))
        client = Consumer({"bootstrap.servers": servers, "group.id": group, "auto.offset.reset": "earliest",
                           "enable.auto.commit": False, "enable.auto.offset.store": False,
                           "allow.auto.create.topics": False, "queued.max.messages.kbytes": 8192,
                           "enable.partition.eof": True})
        try:
            metadata = client.list_topics(timeout=min(10, idle_timeout))
            for topic in ("movie-events", "tmdb-snapshots"):
                if topic not in metadata.topics or metadata.topics[topic].error:
                    raise RuntimeError(f"Required topic unavailable: {topic}")
            client.subscribe(["movie-events", "tmdb-snapshots"])
            idle = time.monotonic()
            while not (stop and stop.requested) and (not limit or stats["received"] < limit):
                message = client.poll(0.2)
                if message is None or (message.error() and message.error().code() == KafkaError._PARTITION_EOF):
                    if time.monotonic() - idle >= idle_timeout:
                        break
                    continue
                if message.error():
                    raise RuntimeError(f"Kafka consumer error: {message.error()}")
                idle = time.monotonic()
                tp = (message.topic(), message.partition())
                if tp in last:
                    if message.offset() <= last[tp]:
                        stats["offset_regressions"] += 1
                    elif message.offset() > last[tp] + 1:
                        stats["offset_gaps"] += message.offset() - last[tp] - 1
                last[tp] = message.offset()
                status, latency = audit.record(message)
                stats["received"] += 1
                stats[status] += 1
                if latency is not None:
                    latency_sum += latency
                    latency_count += 1
                    stats["latency_seconds_max"] = max(stats["latency_seconds_max"] or latency, latency)
                # Invalid/conflicting records are durable in SQLite before advancing.
                committed = client.commit(message=message, asynchronous=False)
                if any(tp.error for tp in committed):
                    raise RuntimeError("Kafka offset commit failed")
            assignment = client.assignment()
            for tp in client.committed(assignment, timeout=10) if assignment else []:
                low, high = client.get_watermark_offsets(tp, timeout=10, cached=False)
                stats["lag"][f"{tp.topic}:{tp.partition}"] = {
                    "low": low, "high": high, "committed": tp.offset,
                    "lag": high - max(low, tp.offset) if tp.offset >= 0 else high - low,
                }
            stats["latency_seconds_mean"] = latency_sum / latency_count if latency_count else None
            stats["elapsed_seconds"] = time.monotonic() - started
            stats["events_per_second"] = stats["received"] / stats["elapsed_seconds"]
            stats["interrupted"] = bool(stop and stop.requested)
            if report:
                atomic_json(report, stats)
            return stats
        finally:
            client.close()
            audit.close()


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--bootstrap", default=bootstrap())
    p.add_argument("--group", default="module02-validator")
    p.add_argument("--audit", type=Path, default=STATE / "consumer.sqlite3")
    p.add_argument("--limit", type=int, default=0)
    p.add_argument("--idle-timeout", type=float, default=10)
    p.add_argument("--report", type=Path)
    args = p.parse_args(argv)
    try:
        with Shutdown() as stop:
            result = consume(args.bootstrap, args.group, args.audit, args.limit, args.idle_timeout, stop, args.report)
        print(json.dumps(result))
        return 130 if result["interrupted"] else int(bool(result["invalid"] or result["conflict"] or result["offset_regressions"]))
    except Exception as exc:
        print(f"CONSUMER_FAIL: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
