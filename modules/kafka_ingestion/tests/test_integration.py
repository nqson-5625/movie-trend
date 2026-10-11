"""Opt-in tests use real Kafka and real Module 01 prepared data."""
import csv
import json
import os
import threading
from pathlib import Path

import pytest
from confluent_kafka import Producer
from modules.kafka_ingestion.src.local_test_broker import TestBroker
from modules.kafka_ingestion.src.movie_producer import replay, DEFAULT_RATINGS
from modules.kafka_ingestion.src.tmdb_producer import publish
from modules.kafka_ingestion.src.consumer import consume
from modules.kafka_ingestion.src.common import ROOT, Delivery, utc_now
from modules.data_source.src.movie_event_adapter import to_movie_event

pytestmark = pytest.mark.skipif(os.getenv("RUN_KAFKA_INTEGRATION") != "1", reason="Use make integration")


@pytest.fixture(scope="module")
def broker(tmp_path_factory):
    broker = TestBroker(tmp_path_factory.mktemp("kafka-real"))
    try:
        broker.start()
        broker.topics()
        yield broker
    finally:
        broker.stop()


def test_real_pipeline_offsets_duplicates_bad_json_and_restart(broker, tmp_path):
    checkpoint = tmp_path / "movie.json"
    first = replay(DEFAULT_RATINGS, checkpoint, broker.bootstrap, rate=500, batch_size=10, limit=20)
    assert first["acknowledged"] == 20
    received = consume(broker.bootstrap, "integration", tmp_path / "audit.db", limit=20)
    assert received["valid"] == 20 and received["invalid"] == 0
    assert received["lag"]["movie-events:0"]["lag"] == 0
    broker.stop()
    broker.start()
    assert consume(broker.bootstrap, "integration", tmp_path / "audit.db", idle_timeout=2)["received"] == 0
    # Same input, new checkpoint explicitly replays; durable event IDs detect duplicates.
    replay(DEFAULT_RATINGS, tmp_path / "again.json", broker.bootstrap, rate=500, limit=20)
    producer = Producer({"bootstrap.servers": broker.bootstrap})
    producer.produce("movie-events", key=b"1", value=b'{invalid JSON')
    producer.produce("movie-events", key=b"1", value=b'{}')
    assert producer.flush(10) == 0
    received = consume(broker.bootstrap, "integration", tmp_path / "audit.db", limit=22)
    assert received["duplicate"] == 20 and received["invalid"] == 2
    assert received["offset_regressions"] == 0
    # Resume existing cursor produces only the next source rows.
    assert replay(DEFAULT_RATINGS, checkpoint, broker.bootstrap, rate=500, limit=5)["next_row"] == 25
    assert consume(broker.bootstrap, "integration", tmp_path / "audit.db", limit=5)["valid"] == 5


def test_disconnect_no_checkpoint_then_recover(broker, tmp_path):
    checkpoint = tmp_path / "resume.json"
    broker.stop()
    try:
        with pytest.raises(RuntimeError, match="delivery incomplete"):
            replay(DEFAULT_RATINGS, checkpoint, broker.bootstrap, rate=500, limit=3, timeout=2)
        assert not checkpoint.exists()
        # An unavailable broker must not look like a successful empty consume.
        with pytest.raises(Exception):
            consume(broker.bootstrap, "offline", tmp_path / "offline.db", idle_timeout=1)
    finally:
        broker.start()
    result = replay(DEFAULT_RATINGS, checkpoint, broker.bootstrap, rate=500, limit=3)
    assert result["acknowledged"] == 3 and result["next_row"] == 3


def test_delivery_retries_during_broker_restart(broker):
    with DEFAULT_RATINGS.open() as stream:
        event = to_movie_event(next(csv.DictReader(stream)), utc_now())
    sender = Delivery(broker.bootstrap, timeout=45)
    # Establish producer connection before interruption.
    sender.send([event], "movie-events")
    broker.stop()
    failures = []
    def send():
        try:
            sender.send([event], "movie-events")
        except Exception as exc:
            failures.append(exc)
    worker = threading.Thread(target=send)
    worker.start()
    try:
        broker.start()
    finally:
        worker.join(timeout=50)
    assert not worker.is_alive() and not failures
    assert sender.stats["acknowledged"] == 2


def test_real_tmdb_snapshot_through_module01_adapter(broker, tmp_path, monkeypatch):
    # Real saved Module 01 snapshot, so CI does not need a credential/network.
    snapshot = sorted((ROOT / "data/prepared/tmdb").glob("snapshot_*.jsonl"))[-1]
    records = [json.loads(line) for line in snapshot.read_text().splitlines()]
    raw = [dict(id=e["tmdbId"], title=e["title"], popularity=e["popularity"], vote_count=e["vote_count"], vote_average=e["vote_average"]) for e in records]
    monkeypatch.setenv("TMDB_API_KEY", "offline-real-snapshot")
    result = publish(tmp_path / "tmdb.json", broker.bootstrap, fetch=lambda key: (raw, records[0]["event_time"]))
    assert result["acknowledged"] == len(records)
    stats = consume(broker.bootstrap, "tmdb-integration", tmp_path / "tmdb.db", idle_timeout=3)
    assert stats["lag"]["tmdb-snapshots:0"]["lag"] == 0
    import sqlite3
    with sqlite3.connect(tmp_path / "tmdb.db") as db:
        assert db.execute("SELECT COUNT(*) FROM messages WHERE topic='tmdb-snapshots' AND status='valid'").fetchone()[0] == len(records)


def test_sigterm_flushes_acknowledged_prefix_and_resumes(broker, tmp_path):
    import signal
    import subprocess
    import sys
    import time
    checkpoint = tmp_path / "signal.json"
    proc = subprocess.Popen([
        sys.executable, "-m", "modules.kafka_ingestion.src.movie_producer",
        "--bootstrap", broker.bootstrap, "--checkpoint", str(checkpoint),
        "--rate", "10", "--batch-size", "2", "--limit", "100",
    ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        deadline = time.monotonic() + 15
        while not checkpoint.exists() and time.monotonic() < deadline and proc.poll() is None:
            time.sleep(0.05)
        assert checkpoint.exists()
        proc.send_signal(signal.SIGTERM)
        out, err = proc.communicate(timeout=15)
        assert proc.returncode == 130, err
        summary = json.loads(out)
        cursor = json.loads(checkpoint.read_text())["next_row"]
        assert cursor == summary["acknowledged"] and 0 < cursor < 100
        result = replay(DEFAULT_RATINGS, checkpoint, broker.bootstrap, rate=500, limit=2)
        assert result["next_row"] == cursor + 2
    finally:
        if proc.poll() is None:
            proc.terminate()
            proc.wait(timeout=15)
