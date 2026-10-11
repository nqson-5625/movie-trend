"""Broker lifecycle unit tests: never require or contact a real Kafka broker."""

from pathlib import Path
from types import SimpleNamespace

import pytest

from modules.kafka_ingestion.src import kafka_lifecycle as lifecycle


@pytest.fixture
def settings(tmp_path):
    kafka_home = tmp_path / "kafka"
    start_script = kafka_home / "bin" / "kafka-server-start.sh"
    start_script.parent.mkdir(parents=True)
    start_script.write_text("#!/bin/sh\nexit 0\n")
    start_script.chmod(0o755)
    cfg_dir = tmp_path / "kafka-local" / "config"
    cfg_dir.mkdir(parents=True)
    data = tmp_path / "kafka-local" / "data"
    data.mkdir()
    config = cfg_dir / "server.properties"
    config.write_text(f"process.roles=broker,controller\nnode.id=1\nlog.dirs={data}\n")
    (data / "meta.properties").write_text("node.id=1\ncluster.id=test-cluster-id\n")
    return lifecycle.Settings(
        kafka_home=kafka_home,
        config=config,
        log_dir=tmp_path / "logs",
        bootstrap="127.0.0.1:9092",
        heap="-Xms512m -Xmx512m",
        startup_timeout=2,
        shutdown_timeout=2,
    )


def test_validate_storage(settings):
    assert lifecycle.validate_setup(settings) == settings.config.parent.parent / "data"


def test_refuses_unformatted_storage(settings, monkeypatch):
    (settings.config.parent.parent / "data" / "meta.properties").unlink()
    monkeypatch.setattr(lifecycle.subprocess, "Popen", lambda *a, **k: pytest.fail("Must not start Kafka"))
    with pytest.raises(RuntimeError, match="STORAGE_NOT_FORMATTED"):
        lifecycle.start(settings)


def test_rejects_remote_bootstrap():
    with pytest.raises(ValueError, match="loopback"):
        lifecycle.address("kafka.example.org:9092")
    assert lifecycle.address("127.0.0.1:9092") == ("127.0.0.1", 9092)


def test_find_process_matches_only_exact_config(tmp_path):
    proc = tmp_path / "proc"
    proc.mkdir()
    cfg = tmp_path / "correct.properties"
    cfg.write_text("example")
    for pid, args in [(123, ["java", "kafka.Kafka", str(cfg)]),
                      (124, ["java", "kafka.Kafka", str(tmp_path / "wrong.properties")]),
                      (125, ["java", "OtherClass", str(cfg)])]:
        folder = proc / str(pid)
        folder.mkdir()
        (folder / "cmdline").write_bytes("\0".join(args).encode() + b"\0")
        (folder / "stat").write_text(f"{pid} (java) S 0 0 0")
    assert lifecycle.find_matching_pids(cfg, proc) == [123]


def test_start_idempotent_when_running(settings, monkeypatch):
    monkeypatch.setattr(lifecycle, "find_matching_pids", lambda _: [123])
    monkeypatch.setattr(lifecycle, "broker_ready", lambda _: True)
    monkeypatch.setattr(lifecycle.subprocess, "Popen", lambda *a, **k: pytest.fail("Must not launch"))
    assert lifecycle.start(settings) == 0


def test_start_refuses_foreign_port(settings, monkeypatch):
    monkeypatch.setattr(lifecycle, "find_matching_pids", lambda _: [])
    monkeypatch.setattr(lifecycle, "port_open", lambda _: True)
    with pytest.raises(RuntimeError, match="PORT_IN_USE"):
        lifecycle.start(settings)


def test_start_uses_safe_process_options(settings, monkeypatch):
    saved = {}

    def fake_popen(args, **kwargs):
        saved["args"] = args
        saved["kwargs"] = kwargs
        return SimpleNamespace(pid=123, poll=lambda: None)

    monkeypatch.setattr(lifecycle, "find_matching_pids", lambda _: [])
    monkeypatch.setattr(lifecycle, "port_open", lambda _: False)
    monkeypatch.setattr(lifecycle, "broker_ready", lambda _: True)
    monkeypatch.setattr(lifecycle.subprocess, "Popen", fake_popen)
    assert lifecycle.start(settings) == 0
    assert saved["kwargs"]["start_new_session"] is True
    assert saved["kwargs"]["env"]["KAFKA_HEAP_OPTS"] == "-Xms512m -Xmx512m"
    assert saved["args"][-1] == str(settings.config)


def test_stop_never_kills_unrelated_process(settings, monkeypatch):
    monkeypatch.setattr(lifecycle, "find_matching_pids", lambda _: [])
    monkeypatch.setattr(lifecycle.os, "kill", lambda *_: pytest.fail("Must not signal process"))
    assert lifecycle.stop(settings) == 0


def test_stop_uses_sigterm_only(settings, monkeypatch):
    seen = []
    monkeypatch.setattr(lifecycle, "find_matching_pids", lambda _: [456])
    monkeypatch.setattr(lifecycle.os, "kill", lambda pid, sig: seen.append((pid, sig)))
    monkeypatch.setattr(lifecycle, "is_alive", lambda _: False)
    assert lifecycle.stop(settings) == 0
    assert seen == [(456, lifecycle.signal.SIGTERM)]


def test_status_stopped_reports_failure(settings, monkeypatch, capsys):
    monkeypatch.setattr(lifecycle, "find_matching_pids", lambda _: [])
    monkeypatch.setattr(lifecycle, "port_open", lambda _: False)
    assert lifecycle.status(settings) == 1
    assert "KAFKA_STOPPED" in capsys.readouterr().out
