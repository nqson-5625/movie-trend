"""Pure unit tests: do not connect to a live Kafka broker."""

import json
from types import SimpleNamespace

import pytest

from modules.kafka_ingestion.src.admin_topics import TopicSpec, load_specs, process_topics


def fake_topic(partitions=1, replication_factor=1):
    part = SimpleNamespace(replicas={i: object() for i in range(replication_factor)})
    return SimpleNamespace(partitions={i: part for i in range(partitions)}, error=None)


class FakeFuture:
    def result(self):
        return None


class FakeAdmin:
    def __init__(self, topics=None):
        self.topics = topics or {}
        self.create_calls = 0

    def list_topics(self, timeout=None):
        return SimpleNamespace(topics=dict(self.topics))

    def create_topics(self, new_topics, **kwargs):
        self.create_calls += 1
        for t in new_topics:
            self.topics[t.topic] = fake_topic(t.num_partitions, t.replication_factor)
        return {t.topic: FakeFuture() for t in new_topics}


def test_topics_config_contract():
    from modules.kafka_ingestion.src.admin_topics import DEFAULT_CONFIG

    specs = load_specs(DEFAULT_CONFIG)
    assert [spec.name for spec in specs] == ["movie-events", "tmdb-snapshots"]
    assert all(spec.partitions == 1 and spec.replication_factor == 1 for spec in specs)


def test_apply_creates_topics_and_is_idempotent(capsys):
    admin = FakeAdmin()
    specs = [TopicSpec("movie-events", 1, 1), TopicSpec("tmdb-snapshots", 1, 1)]
    process_topics(admin, specs, "apply")
    process_topics(admin, specs, "apply")
    assert admin.create_calls == 1
    assert "NO_CHANGES_NEEDED" in capsys.readouterr().out


def test_check_is_read_only_and_fails_when_missing():
    admin = FakeAdmin()
    with pytest.raises(RuntimeError, match="MISSING_TOPICS"):
        process_topics(admin, [TopicSpec("movie-events", 1, 1)], "check")
    assert admin.create_calls == 0


def test_drift_does_not_change_existing_topic():
    admin = FakeAdmin({"movie-events": fake_topic(3, 1)})
    with pytest.raises(RuntimeError, match="CONFIG_MISMATCH"):
        process_topics(admin, [TopicSpec("movie-events", 1, 1)], "apply")
    assert admin.create_calls == 0


def test_reject_duplicate_topics(tmp_path):
    path = tmp_path / "topics.json"
    path.write_text(json.dumps({"topics": [
        {"name": "movie-events", "partitions": 1, "replication_factor": 1},
        {"name": "movie-events", "partitions": 1, "replication_factor": 1}
    ]}))
    with pytest.raises(ValueError, match="Duplicate"):
        load_specs(path)
