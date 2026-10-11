"""Safely create and check Module 02 Kafka topics.

Run from repository root:
    python -m modules.kafka_ingestion.src.admin_topics apply
    python -m modules.kafka_ingestion.src.admin_topics check

Only creates missing topics; never deletes topics, changes partitions or
replication factors, or overwrites existing data.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[3] / ".env", override=False)

from confluent_kafka.admin import AdminClient, NewTopic

DEFAULT_CONFIG = Path(__file__).resolve().parents[1] / "config" / "topics.json"
TOPIC_NAME = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")


@dataclass(frozen=True)
class TopicSpec:
    name: str
    partitions: int
    replication_factor: int


def load_specs(path: Path) -> list[TopicSpec]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or not isinstance(payload.get("topics"), list):
        raise ValueError("Config must be an object containing a topics list")

    specs: list[TopicSpec] = []
    names: set[str] = set()
    for obj in payload["topics"]:
        if not isinstance(obj, dict):
            raise ValueError("Each topic must be an object")
        name = obj.get("name")
        partitions = obj.get("partitions")
        replicas = obj.get("replication_factor")
        if not isinstance(name, str) or not TOPIC_NAME.fullmatch(name):
            raise ValueError(f"Invalid topic name: {name!r}")
        if name in names:
            raise ValueError(f"Duplicate topic name: {name}")
        if type(partitions) is not int or partitions < 1:
            raise ValueError(f"Invalid partitions for {name}: {partitions!r}")
        if type(replicas) is not int or replicas < 1:
            raise ValueError(f"Invalid replication_factor for {name}: {replicas!r}")
        names.add(name)
        specs.append(TopicSpec(name, partitions, replicas))
    if not specs:
        raise ValueError("Topic config cannot be empty")
    return specs


def validate_existing(topic_meta: object, spec: TopicSpec) -> None:
    """Reject drift; never silently change a pre-existing topic."""
    error = getattr(topic_meta, "error", None)
    if error:
        raise RuntimeError(f"Kafka metadata error for {spec.name}: {error}")
    partitions = topic_meta.partitions
    count = len(partitions)
    replicas_per_partition = {len(part.replicas) for part in partitions.values()}
    if count != spec.partitions or replicas_per_partition != {spec.replication_factor}:
        raise RuntimeError(
            f"CONFIG_MISMATCH {spec.name}: existing partitions={count}, "
            f"replication_factors={sorted(replicas_per_partition)}; "
            f"expected partitions={spec.partitions}, "
            f"replication_factor={spec.replication_factor}. "
            "Stop and review; no automatic destructive changes."
        )


def process_topics(admin: AdminClient, specs: list[TopicSpec], action: str) -> None:
    if action not in {"apply", "check"}:
        raise ValueError(f"Unknown action: {action}")

    meta = admin.list_topics(timeout=10)
    known = meta.topics
    missing = [spec for spec in specs if spec.name not in known]
    for spec in specs:
        if spec.name in known:
            validate_existing(known[spec.name], spec)
            print(f"EXISTS_OK {spec.name} ({spec.partitions} partitions, RF={spec.replication_factor})")

    if action == "check":
        if missing:
            raise RuntimeError("MISSING_TOPICS: " + ", ".join(s.name for s in missing))
        print("TOPICS_CHECK_PASS")
        return

    if missing:
        print("CREATING: " + ", ".join(spec.name for spec in missing))
        futures = admin.create_topics(
            [
                NewTopic(
                    spec.name,
                    num_partitions=spec.partitions,
                    replication_factor=spec.replication_factor,
                )
                for spec in missing
            ],
            operation_timeout=30,
            request_timeout=40,
        )
        for spec in missing:
            # The Admin API is async. Wait for the broker's result.
            futures[spec.name].result()
            print(f"CREATED {spec.name}")
    else:
        print("NO_CHANGES_NEEDED")

    # Verify the resulting metadata, including partition count and replica factor.
    refreshed = admin.list_topics(timeout=10).topics
    for spec in specs:
        if spec.name not in refreshed:
            raise RuntimeError(f"Topic not visible after apply: {spec.name}")
        validate_existing(refreshed[spec.name], spec)
    print("TOPICS_APPLY_PASS")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["apply", "check"])
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument(
        "--bootstrap",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
        help="Kafka bootstrap host:port (default: env or 127.0.0.1:9092)",
    )
    args = parser.parse_args(argv)
    try:
        specs = load_specs(args.config)
        print(f"BOOTSTRAP {args.bootstrap}")
        client = AdminClient({"bootstrap.servers": args.bootstrap, "socket.timeout.ms": 10000})
        process_topics(client, specs, args.action)
        return 0
    except Exception as exc:
        # Includes broker errors from confluent-kafka. All errors are fatal.
        print(f"TOPICS_{args.action.upper()}_FAIL: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
