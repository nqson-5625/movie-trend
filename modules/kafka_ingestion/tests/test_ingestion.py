import json
from types import SimpleNamespace
import pytest
from modules.kafka_ingestion.src.common import Delivery, decode, validate, locked
from modules.kafka_ingestion.src.movie_producer import replay
from modules.kafka_ingestion.src.tmdb_producer import publish
from modules.kafka_ingestion.src.consumer import Audit
from modules.data_source.src.movie_event_adapter import to_movie_event


def event():
    return to_movie_event(dict(userId=1, movieId=2, rating=4, timestamp=1000000000), "2026-10-11T01:00:00Z")


class Sender:
    def __init__(self, fail=False):
        self.stats = {"acknowledged": 0}
        self.events = []
        self.fail = fail

    def send(self, events, topic, *args, **kwargs):
        self.events.extend(events)
        if self.fail:
            raise RuntimeError("ambiguous delivery")
        self.stats["acknowledged"] += len(self.events)
        return len(self.events)


def ratings(tmp_path):
    path = tmp_path / "ratings.csv"
    path.write_text("userId,movieId,rating,timestamp\n1,2,4,1000000000\n2,3,5,1000000001\n")
    return path


def test_resume_and_changed_input(tmp_path):
    source, checkpoint = ratings(tmp_path), tmp_path / "state.json"
    first, second = Sender(), Sender()
    assert replay(source, checkpoint, "broker", limit=1, delivery=first)["next_row"] == 1
    assert replay(source, checkpoint, "broker", delivery=second)["next_row"] == 2
    assert first.events[0]["event_id"] != second.events[0]["event_id"]
    source.write_text(source.read_text() + "3,4,3,1000000002\n")
    with pytest.raises(ValueError, match="changed"):
        replay(source, checkpoint, "broker", delivery=Sender())


def test_failed_batch_does_not_checkpoint(tmp_path):
    checkpoint = tmp_path / "state.json"
    with pytest.raises(RuntimeError):
        replay(ratings(tmp_path), checkpoint, "broker", delivery=Sender(True))
    assert not checkpoint.exists()


def test_lock_prevents_concurrent_replay(tmp_path):
    with locked(tmp_path / "state"):
        with pytest.raises(RuntimeError, match="already in use"):
            with locked(tmp_path / "state"):
                pass


@pytest.mark.parametrize("raw", [b'{"rating":NaN}', b'{"a":1,"a":2}', b'\xff', b'{'])
def test_strict_json(raw):
    with pytest.raises((ValueError, UnicodeError)):
        decode(raw)


def test_identity_and_key():
    assert validate(event(), "movie-events", b"2") == b"2"
    with pytest.raises(ValueError):
        validate(event(), "movie-events", b"1")
    changed = event() | {"event_id": "0" * 64}
    with pytest.raises(ValueError):
        validate(changed, "movie-events")


def test_delivery_requires_callback():
    class Client:
        def produce(self, *args, **kwargs): pass
        def poll(self, timeout): pass
        def flush(self, timeout): return 0
    sender = Delivery("broker", producer=Client())
    with pytest.raises(RuntimeError, match="incomplete"):
        sender.send([event()], "movie-events")
    assert sender.stats["unconfirmed"] == 1


def test_terminal_callback_failure():
    class Client:
        def produce(self, *args, **kwargs): kwargs["on_delivery"]("timeout", None)
        def poll(self, timeout): pass
        def flush(self, timeout): return 0
    sender = Delivery("broker", producer=Client())
    with pytest.raises(RuntimeError):
        sender.send([event()], "movie-events")
    assert sender.stats["failed"] == 1


def test_tmdb_resume_same_snapshot_and_cooldown(tmp_path, monkeypatch):
    monkeypatch.setenv("TMDB_API_KEY", "test")
    calls = []
    def fetch(key):
        calls.append(key)
        return [dict(id=1, title="Movie", popularity=1, vote_count=2, vote_average=5)], "2026-10-11T01:00:00Z"
    path = tmp_path / "tmdb.json"
    failed = Sender(True)
    with pytest.raises(RuntimeError):
        publish(path, "broker", fetch=fetch, delivery=failed)
    good = Sender()
    assert publish(path, "broker", fetch=fetch, delivery=good)["status"] == "complete"
    assert failed.events[0]["event_id"] == good.events[0]["event_id"]
    assert publish(path, "broker", fetch=fetch)["status"] == "cooldown"
    assert len(calls) == 1


def test_tmdb_auth_no_retry(tmp_path, monkeypatch):
    monkeypatch.setenv("TMDB_API_KEY", "test")
    calls = []
    def fetch(key):
        calls.append(key)
        raise RuntimeError("TMDB HTTP 401")
    with pytest.raises(RuntimeError):
        publish(tmp_path / "tmdb.json", "broker", fetch=fetch)
    assert len(calls) == 1


def test_tmdb_transient_retry(tmp_path, monkeypatch):
    monkeypatch.setenv("TMDB_API_KEY", "test")
    calls = []
    def fetch(key):
        calls.append(key)
        if len(calls) == 1:
            raise RuntimeError("TMDB HTTP 503")
        return [dict(id=1, title="Movie", popularity=1, vote_count=2, vote_average=5)], "2026-10-11T01:00:00Z"
    publish(tmp_path / "tmdb.json", "broker", fetch=fetch, delivery=Sender())
    assert len(calls) == 2


def test_durable_duplicate_conflict_and_invalid(tmp_path):
    path = tmp_path / "audit.db"
    def message(value, offset):
        return SimpleNamespace(topic=lambda: "movie-events", partition=lambda: 0,
                               offset=lambda: offset, value=lambda: value, key=lambda: b"2")
    audit = Audit(path, "test")
    assert audit.record(message(json.dumps(event()).encode(), 0))[0] == "valid"
    audit.close()
    audit = Audit(path, "test")
    assert audit.record(message(json.dumps(event()).encode(), 1))[0] == "duplicate"
    assert audit.record(message(json.dumps(event() | {"rating": 5}).encode(), 2))[0] == "conflict"
    assert audit.record(message(b'{', 3))[0] == "invalid"
    assert audit.db.execute("SELECT COUNT(*) FROM messages").fetchone()[0] == 4
    audit.close()


def test_partial_callbacks_never_advance_failed_batch(tmp_path):
    class Client:
        count = 0
        def produce(self, *args, **kwargs):
            self.count += 1
            kwargs["on_delivery"](None if self.count == 1 else "timeout", None)
        def poll(self, timeout): pass
        def flush(self, timeout): return 0
    sender = Delivery("broker", producer=Client())
    checkpoint = tmp_path / "state.json"
    with pytest.raises(RuntimeError):
        replay(ratings(tmp_path), checkpoint, "broker", rate=10000, delivery=sender)
    assert sender.stats["acknowledged"] == 1 and sender.stats["failed"] == 1
    assert not checkpoint.exists()


def test_out_of_order_callbacks_require_all_acks():
    class Client:
        callbacks = []
        def produce(self, *args, **kwargs): self.callbacks.append(kwargs["on_delivery"])
        def poll(self, timeout): pass
        def flush(self, timeout):
            for callback in reversed(self.callbacks):
                callback(None, None)
            return 0
    sender = Delivery("broker", producer=Client())
    assert sender.send([event(), event()], "movie-events") == 2
    assert sender.stats["acknowledged"] == 2
