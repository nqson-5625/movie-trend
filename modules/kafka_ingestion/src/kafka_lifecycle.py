"""Start, stop and inspect a *local* Kafka broker (development only).

Uses Python's subprocess management to launch the Kafka script in the
foreground, but detached from the terminal. The Kafka Java process survives
VS Code terminal closure while WSL2 is running. No storage formatting and no
Kafka data deletion are performed.

Examples (from repository root):
    python -m modules.kafka_ingestion.src.kafka_lifecycle start
    python -m modules.kafka_ingestion.src.kafka_lifecycle status
    python -m modules.kafka_ingestion.src.kafka_lifecycle stop
"""

from __future__ import annotations

import argparse
import os
import signal
import socket
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[3] / ".env", override=False)

DEFAULT_HOME = Path.home() / "kafka-local"


@dataclass(frozen=True)
class Settings:
    kafka_home: Path
    config: Path
    log_dir: Path
    bootstrap: str
    heap: str
    startup_timeout: float = 60
    shutdown_timeout: float = 30

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            kafka_home=Path(os.getenv("KAFKA_HOME", "/opt/kafka")).expanduser().resolve(),
            config=Path(
                os.getenv("KAFKA_CONFIG", str(DEFAULT_HOME / "config" / "server.properties"))
            ).expanduser().resolve(),
            log_dir=Path(os.getenv("KAFKA_LOG_DIR", str(DEFAULT_HOME / "logs"))).expanduser().resolve(),
            bootstrap=os.getenv("KAFKA_LOCAL_BOOTSTRAP", "127.0.0.1:9092"),
            heap=os.getenv("KAFKA_HEAP_OPTS", "-Xms512m -Xmx512m"),
            startup_timeout=float(os.getenv("KAFKA_START_TIMEOUT", "60")),
            shutdown_timeout=float(os.getenv("KAFKA_STOP_TIMEOUT", "30")),
        )


def load_properties(path: Path) -> dict[str, str]:
    result: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith(("#", "!")):
            continue
        if "=" not in line:
            raise ValueError(f"Invalid properties line: {line!r}")
        key, value = line.split("=", 1)
        key = key.strip()
        if key in result:
            raise ValueError(f"Duplicate properties key: {key}")
        result[key] = value.strip()
    return result


def validate_setup(settings: Settings) -> Path:
    """Require already-formatted storage; NEVER format it automatically."""
    executable = settings.kafka_home / "bin" / "kafka-server-start.sh"
    if not executable.is_file() or not os.access(executable, os.X_OK):
        raise RuntimeError(f"Kafka start script missing/not executable: {executable}")
    if not settings.config.is_file():
        raise RuntimeError(f"Kafka config missing: {settings.config}")

    props = load_properties(settings.config)
    if props.get("process.roles") != "broker,controller" or props.get("node.id") != "1":
        raise RuntimeError("Expected existing local single-node broker,controller with node.id=1")
    directories = props.get("log.dirs", props.get("log.dir", ""))
    if not directories or "," in directories:
        raise RuntimeError("Expected exactly one Kafka log.dirs directory")
    data_dir = Path(directories).expanduser().resolve()
    metadata = data_dir / "meta.properties"
    if not metadata.is_file():
        raise RuntimeError(f"STORAGE_NOT_FORMATTED: missing {metadata}; refusing to start")
    meta_props = load_properties(metadata)
    if meta_props.get("node.id") != "1" or not meta_props.get("cluster.id"):
        raise RuntimeError("Unexpected meta.properties; refusing to start")
    return data_dir


def find_matching_pids(config: Path, proc: Path = Path("/proc")) -> list[int]:
    """Match ONLY Java processes executing kafka.Kafka with THIS config path."""
    found: list[int] = []
    for entry in proc.iterdir():
        if not entry.name.isdecimal():
            continue
        try:
            argv = (entry / "cmdline").read_bytes().decode("utf-8", "replace").split("\0")
            if "kafka.Kafka" not in argv:
                continue
            index = argv.index("kafka.Kafka")
            if index + 1 < len(argv) and Path(argv[index + 1]).expanduser().resolve() == config:
                if (entry / "stat").read_text().split()[2] != "Z":
                    found.append(int(entry.name))
        except (OSError, PermissionError, IndexError, ValueError):
            continue
    return sorted(found)


def is_alive(pid: int) -> bool:
    try:
        status = Path(f"/proc/{pid}/stat").read_text().split()[2]
        return status != "Z"
    except (OSError, IndexError):
        return False


def address(bootstrap: str) -> tuple[str, int]:
    try:
        host, port_string = bootstrap.rsplit(":", 1)
        port = int(port_string)
        if host not in {"127.0.0.1", "localhost", "::1"} or not 1 <= port <= 65535:
            raise ValueError
        return host, port
    except ValueError as exc:
        raise ValueError("KAFKA_LOCAL_BOOTSTRAP must be a loopback host:port") from exc


def port_open(bootstrap: str, timeout: float = 0.5) -> bool:
    host, port = address(bootstrap)
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def broker_ready(bootstrap: str) -> bool:
    """Check Kafka metadata, not merely whether a TCP port accepts sockets."""
    if not port_open(bootstrap):
        return False
    from confluent_kafka.admin import AdminClient

    try:
        client = AdminClient(
            {"bootstrap.servers": bootstrap, "socket.timeout.ms": 1800, "log_level": 0}
        )
        cluster = client.list_topics(timeout=2.5)
        return bool(cluster.brokers)
    except Exception:
        return False


def start(settings: Settings) -> int:
    validate_setup(settings)
    matches = find_matching_pids(settings.config)
    if len(matches) > 1:
        raise RuntimeError(f"MULTIPLE_MATCHING_KAFKA_PROCESSES: {matches}")
    if matches:
        if broker_ready(settings.bootstrap):
            print(f"KAFKA_ALREADY_RUNNING pid={matches[0]}")
            return 0
        raise RuntimeError(f"KAFKA_PROCESS_PRESENT_BUT_NOT_READY pid={matches[0]}; inspect logs")
    if port_open(settings.bootstrap):
        raise RuntimeError(f"PORT_IN_USE {settings.bootstrap}; refusing to launch another Kafka")

    settings.log_dir.mkdir(parents=True, exist_ok=True)
    log_file = settings.log_dir / "kafka-lifecycle.out"
    env = os.environ.copy()
    env["KAFKA_HEAP_OPTS"] = settings.heap
    env["LOG_DIR"] = str(settings.log_dir)
    with log_file.open("ab") as out:
        proc = subprocess.Popen(
            [str(settings.kafka_home / "bin" / "kafka-server-start.sh"), str(settings.config)],
            stdin=subprocess.DEVNULL,
            stdout=out,
            stderr=subprocess.STDOUT,
            cwd=str(settings.kafka_home),
            env=env,
            start_new_session=True,
        )
    print(f"KAFKA_STARTING launcher_pid={proc.pid}; log={log_file}", flush=True)
    deadline = time.monotonic() + settings.startup_timeout
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"KAFKA_EXITED_EARLY rc={proc.returncode}; check {log_file}")
        if broker_ready(settings.bootstrap):
            print("KAFKA_READY")
            return 0
        time.sleep(1)
    raise RuntimeError(f"KAFKA_START_TIMEOUT; check {log_file}; do not start again blindly")


def status(settings: Settings) -> int:
    matches = find_matching_pids(settings.config)
    if not matches:
        if port_open(settings.bootstrap):
            print("KAFKA_NOT_OURS_PORT_OCCUPIED")
        else:
            print("KAFKA_STOPPED")
        return 1
    if len(matches) > 1:
        print(f"KAFKA_MULTIPLE_MATCHING_PROCESSES: {matches}")
        return 1
    if broker_ready(settings.bootstrap):
        print(f"KAFKA_RUNNING pid={matches[0]} bootstrap={settings.bootstrap}")
        return 0
    print(f"KAFKA_NOT_READY pid={matches[0]}; inspect logs")
    return 1


def stop(settings: Settings) -> int:
    matches = find_matching_pids(settings.config)
    if not matches:
        print("KAFKA_ALREADY_STOPPED")
        return 0
    if len(matches) != 1:
        raise RuntimeError(f"MULTIPLE_MATCHING_KAFKA_PROCESSES: {matches}; refusing to stop")
    pid = matches[0]
    print(f"KAFKA_STOPPING pid={pid}", flush=True)
    os.kill(pid, signal.SIGTERM)  # Graceful shutdown; never SIGKILL automatically.
    deadline = time.monotonic() + settings.shutdown_timeout
    while time.monotonic() < deadline:
        if not is_alive(pid):
            print("KAFKA_STOPPED_OK")
            return 0
        time.sleep(0.5)
    raise RuntimeError(f"KAFKA_STOP_TIMEOUT pid={pid}; inspect logs before retrying")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["start", "status", "stop"])
    args = parser.parse_args(argv)
    try:
        settings = Settings.from_env()
        address(settings.bootstrap)
        if args.action == "start":
            return start(settings)
        if args.action == "stop":
            return stop(settings)
        return status(settings)
    except Exception as exc:
        print(f"KAFKA_{args.action.upper()}_FAIL: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
