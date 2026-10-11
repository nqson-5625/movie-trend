"""Shared wire validation, durable local state and bounded Kafka delivery."""
from __future__ import annotations

import fcntl
import hashlib
import json
import os
import signal
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Producer
from dotenv import load_dotenv
from jsonschema import Draft202012Validator, FormatChecker

from modules.data_source.src.movie_event_adapter import build_movie_event_id
from modules.data_source.src.tmdb_source_adapter import build_tmdb_event_id

ROOT = Path(__file__).resolve().parents[3]
STATE = ROOT / "data/local/kafka_ingestion"
load_dotenv(ROOT / ".env", override=False)
VALIDATORS = {
    topic: Draft202012Validator(json.loads((ROOT / "contracts/schemas" / f"{topic}.schema.json").read_text()), format_checker=FormatChecker())
    for topic in ("movie-events", "tmdb-snapshots")
}


def utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def bootstrap():
    return os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")


def validate(event, topic, key=None):
    VALIDATORS[topic].validate(event)
    if topic == "movie-events":
        timestamp = int(datetime.fromisoformat(event["event_time"].replace("Z", "+00:00")).timestamp())
        expected = build_movie_event_id(event["userId"], event["movieId"], timestamp)
        expected_key = str(event["movieId"]).encode()
    else:
        expected = build_tmdb_event_id(event["tmdbId"], event["event_time"])
        expected_key = str(event["tmdbId"]).encode()
    if event["event_id"] != expected:
        raise ValueError("event_id does not match source identity")
    if key is not None and key != expected_key:
        raise ValueError("Kafka key does not match contract")
    return expected_key


def decode(raw):
    def reject(value):
        raise ValueError(f"Non-finite JSON number: {value}")
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("Duplicate JSON field")
            result[key] = value
        return result
    return json.loads(raw, parse_constant=reject, object_pairs_hook=pairs)


def digest(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("w", encoding="utf-8") as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2, allow_nan=False)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(tmp, path)
    fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


@contextmanager
def locked(path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.with_name(path.name + ".lock").open("a") as stream:
        try:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError(f"State already in use: {path}") from None
        yield


class Shutdown:
    def __init__(self):
        self.requested = False

    def __enter__(self):
        self.previous = {sig: signal.signal(sig, self.handle) for sig in (signal.SIGINT, signal.SIGTERM)}
        return self

    def handle(self, *_):
        self.requested = True

    def __exit__(self, *_):
        for sig, handler in self.previous.items():
            signal.signal(sig, handler)


class Delivery:
    """A batch succeeds only after every delivery callback reports success.

    librdkafka retries within delivery.timeout.ms. A terminal/ambiguous error
    aborts the batch; restart from the last checkpoint (at-least-once).
    """
    def __init__(self, servers, timeout=30, producer=None):
        if timeout <= 0:
            raise ValueError("delivery timeout must be positive")
        self.timeout = timeout
        self.client = producer or Producer({
            "bootstrap.servers": servers, "enable.idempotence": True, "acks": "all",
            "delivery.timeout.ms": int(timeout * 1000), "request.timeout.ms": min(5000, int(timeout * 1000)),
            "retry.backoff.ms": 200, "linger.ms": 5, "compression.type": "lz4",
            "queue.buffering.max.kbytes": 8192, "queue.buffering.max.messages": 10000,
            "allow.auto.create.topics": False,
        })
        self.stats = {"attempted": 0, "acknowledged": 0, "failed": 0, "unconfirmed": 0}

    def send(self, events, topic, rate=0, stop=None):
        errors = []
        queued = 0
        callback_count = 0
        started = time.monotonic()

        def delivered(error, message):
            nonlocal callback_count
            callback_count += 1
            if error:
                self.stats["failed"] += 1
                errors.append(str(error))
            else:
                self.stats["acknowledged"] += 1

        for event in events:
            if stop and stop.requested:
                break
            key = validate(event, topic)
            value = json.dumps(event, ensure_ascii=False, allow_nan=False).encode()
            deadline = time.monotonic() + self.timeout
            while True:
                try:
                    self.client.produce(topic, key=key, value=value, on_delivery=delivered)
                    queued += 1
                    self.stats["attempted"] += 1
                    break
                except BufferError:
                    self.client.poll(0.05)
                    if time.monotonic() >= deadline:
                        raise RuntimeError("Producer queue remained full; checkpoint unchanged")
            self.client.poll(0)
            if rate:
                deadline = started + queued / rate
                while time.monotonic() < deadline and not (stop and stop.requested):
                    self.client.poll(0)
                    time.sleep(min(0.05, max(0, deadline - time.monotonic())))
        remaining = self.client.flush(self.timeout + 1)
        self.stats["unconfirmed"] += queued - callback_count
        if remaining or errors or callback_count != queued:
            raise RuntimeError(f"Kafka delivery incomplete: failed={len(errors)}, pending={remaining}; checkpoint unchanged")
        return queued
