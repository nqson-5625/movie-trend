"""Generic immutable-index publisher for Phase 2 versioned analytics aliases."""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

from ..config.es_client import ESClient, PROJECT_ROOT, load_runtime_config, validate_index_name
from .batch_publisher import (
    PublicationError,
    build_atomic_alias_actions,
    build_restore_alias_actions,
    get_current_alias_targets,
    inspect_aliases_response,
)

COMMON_ANALYTICS_FIELDS = ("schema_version", "analytics_run_id", "published_at")
DEFAULT_REPORT_DIR = PROJECT_ROOT / "data" / "local" / "phase2" / "es_reports"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _atomic_write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp, path)


@dataclass(frozen=True)
class VersionedReadyReport:
    alias_name: str
    physical_index: str
    version: str
    analytics_run_id: str
    schema_version: str
    row_count: int
    bulk_error_count: int
    duplicate_key_count: int
    completed_at: str
    status: str = "READY"
    required_mapping_fields: tuple[str, ...] = COMMON_ANALYTICS_FIELDS


def load_versioned_ready_report(path: Path | str) -> VersionedReadyReport:
    report_path = Path(path)
    raw = json.loads(report_path.read_text(encoding="utf-8"))
    required = {
        "alias_name", "physical_index", "version", "schema_version", "row_count",
        "analytics_run_id", "bulk_error_count", "duplicate_key_count", "completed_at", "status",
    }
    missing = sorted(required - set(raw))
    if missing:
        raise ValueError(f"READY report {report_path} thiếu {missing}")
    fields = tuple(raw.get("required_mapping_fields") or COMMON_ANALYTICS_FIELDS)
    return VersionedReadyReport(
        alias_name=str(raw["alias_name"]),
        physical_index=str(raw["physical_index"]),
        version=str(raw["version"]),
        analytics_run_id=str(raw["analytics_run_id"]),
        schema_version=str(raw["schema_version"]),
        row_count=int(raw["row_count"]),
        bulk_error_count=int(raw["bulk_error_count"]),
        duplicate_key_count=int(raw["duplicate_key_count"]),
        completed_at=str(raw["completed_at"]),
        status=str(raw["status"]),
        required_mapping_fields=fields,
    )


def save_versioned_ready_report(report: VersionedReadyReport, path: Path | str) -> None:
    payload = asdict(report)
    payload["required_mapping_fields"] = list(report.required_mapping_fields)
    _atomic_write_json(Path(path), payload)


def validate_versioned_ready_report(report: VersionedReadyReport) -> None:
    cfg = load_runtime_config()
    allowed = set(cfg.get("versioned_publication_aliases") or [])
    alias = validate_index_name(report.alias_name)
    physical = validate_index_name(report.physical_index)
    if alias not in allowed:
        raise ValueError(f"Alias {alias!r} không nằm trong versioned publication allowlist")
    if report.status != "READY":
        raise ValueError("Versioned report phải có status='READY'")
    if report.schema_version != cfg["schema_version"]:
        raise ValueError(
            f"schema_version lệch: {report.schema_version!r} != {cfg['schema_version']!r}"
        )
    if not report.version.strip():
        raise ValueError("version không được rỗng")
    if not report.analytics_run_id.strip():
        raise ValueError("analytics_run_id không được rỗng")
    if not physical.startswith(f"{alias}-v"):
        raise ValueError(f"Physical index phải có dạng {alias}-v<version>, nhận {physical!r}")
    if report.row_count <= 0:
        raise ValueError("row_count phải lớn hơn 0")
    if report.bulk_error_count != 0 or report.duplicate_key_count != 0:
        raise ValueError("Không publish report có Bulk error hoặc duplicate key")
    missing_common = set(COMMON_ANALYTICS_FIELDS) - set(report.required_mapping_fields)
    if missing_common:
        raise ValueError(f"required_mapping_fields thiếu metadata chung {sorted(missing_common)}")


def _mapping_properties(mapping_response: Dict[str, Any], physical_index: str) -> Dict[str, Any]:
    entry = mapping_response.get(physical_index)
    if entry is None and len(mapping_response) == 1:
        entry = next(iter(mapping_response.values()))
    return (entry or {}).get("mappings", {}).get("properties", {})


def verify_versioned_physical_index(
    client: ESClient,
    report: VersionedReadyReport,
) -> Dict[str, Any]:
    validate_versioned_ready_report(report)
    if not client.index_exists(report.physical_index):
        raise ValueError(f"Physical index không tồn tại: {report.physical_index}")
    actual_count = client.count(report.physical_index)
    if actual_count != report.row_count:
        raise ValueError(f"Lệch row_count: report={report.row_count}, ES={actual_count}")
    props = _mapping_properties(client.get_mapping(report.physical_index), report.physical_index)
    missing_fields = sorted(set(report.required_mapping_fields) - set(props))
    if missing_fields:
        raise ValueError(f"Mapping {report.physical_index} thiếu fields {missing_fields}")
    smoke = client.search_smoke(report.physical_index)
    if smoke["count_seen"] != report.row_count:
        raise ValueError(
            f"Physical index chưa searchable đầy đủ: {smoke['count_seen']} != {report.row_count}"
        )
    consistency = client.search(
        report.physical_index,
        {
            "size": 0,
            "aggs": {
                "schema_versions": {"terms": {"field": "schema_version", "size": 2}},
                "analytics_runs": {"terms": {"field": "analytics_run_id", "size": 2}},
                "missing_published_at": {"missing": {"field": "published_at"}},
            },
        },
    ).get("aggregations", {})
    schema_keys = [item["key"] for item in consistency.get("schema_versions", {}).get("buckets", [])]
    run_keys = [item["key"] for item in consistency.get("analytics_runs", {}).get("buckets", [])]
    missing_published = int(consistency.get("missing_published_at", {}).get("doc_count", -1))
    if schema_keys != [report.schema_version]:
        raise ValueError(f"Physical index có schema_version không nhất quán: {schema_keys}")
    if run_keys != [report.analytics_run_id]:
        raise ValueError(f"Physical index có analytics_run_id không nhất quán: {run_keys}")
    if missing_published != 0:
        raise ValueError(f"Physical index có {missing_published} docs thiếu published_at")
    return smoke


def rollback_versioned_alias(
    client: ESClient,
    alias_name: str,
    restore_targets: Iterable[str],
) -> Dict[str, Any]:
    alias = validate_index_name(alias_name)
    restore = [validate_index_name(item) for item in restore_targets]
    current = get_current_alias_targets(client, [alias])
    actions = build_restore_alias_actions(current, {alias: restore})
    if actions:
        inspect_aliases_response(client.update_aliases(actions))
    verified = get_current_alias_targets(client, [alias])
    if verified != {alias: sorted(restore)}:
        raise PublicationError(f"Rollback {alias} không khớp: {verified!r}")
    return {"status": "ROLLED_BACK", "alias": alias, "targets": verified[alias]}


def publish_versioned_alias(
    client: ESClient,
    report: VersionedReadyReport,
    *,
    report_dir: Path | str = DEFAULT_REPORT_DIR,
    dry_run: bool = False,
) -> Dict[str, Any]:
    pre_smoke = verify_versioned_physical_index(client, report)
    before = get_current_alias_targets(client, [report.alias_name])
    actions = build_atomic_alias_actions(
        before,
        {report.alias_name: report.physical_index},
    )
    base = {
        "alias_name": report.alias_name,
        "physical_index": report.physical_index,
        "version": report.version,
        "before": before,
        "actions": actions,
        "pre_publish_smoke": pre_smoke,
    }
    if dry_run:
        return {"status": "DRY_RUN", **base}

    out_dir = Path(report_dir)
    rollback_file = out_dir / f"rollback_{report.alias_name}_{report.version}.json"
    _atomic_write_json(
        rollback_file,
        {"alias_name": report.alias_name, "targets": before[report.alias_name]},
    )
    try:
        if actions:
            inspect_aliases_response(client.update_aliases(actions))
        after = get_current_alias_targets(client, [report.alias_name])
        expected = {report.alias_name: [report.physical_index]}
        if after != expected:
            raise PublicationError(f"Alias verification failed: {after!r} != {expected!r}")
        post_smoke = client.search_smoke(report.alias_name)
        if post_smoke["count_seen"] != report.row_count:
            raise PublicationError("Alias search smoke trả count không khớp READY report")
    except Exception as exc:
        rollback = rollback_versioned_alias(
            client, report.alias_name, before[report.alias_name]
        )
        raise PublicationError(
            f"Publish {report.alias_name} thất bại; rollback={rollback['status']}: {exc}"
        ) from exc

    result = {
        "status": "PUBLISHED",
        **base,
        "after": after,
        "post_publish_smoke": post_smoke,
        "published_at": _utc_now_iso(),
        "rollback_file": str(rollback_file),
    }
    _atomic_write_json(
        out_dir / f"publish_{report.alias_name}_{report.version}.json",
        result,
    )
    return result


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Publish/rollback one versioned analytics alias")
    sub = parser.add_subparsers(dest="command", required=True)
    publish = sub.add_parser("publish")
    publish.add_argument("--ready-report", required=True)
    publish.add_argument("--report-dir", default=str(DEFAULT_REPORT_DIR))
    publish.add_argument("--dry-run", action="store_true")
    rollback = sub.add_parser("rollback")
    rollback.add_argument("--snapshot", required=True)
    args = parser.parse_args(argv)

    try:
        client = ESClient(owner="admin")
        if args.command == "publish":
            report = load_versioned_ready_report(args.ready_report)
            result = publish_versioned_alias(
                client,
                report,
                report_dir=Path(args.report_dir),
                dry_run=args.dry_run,
            )
        else:
            snapshot = json.loads(Path(args.snapshot).read_text(encoding="utf-8"))
            result = rollback_versioned_alias(
                client,
                str(snapshot["alias_name"]),
                snapshot.get("targets") or [],
            )
    except Exception as exc:  # noqa: BLE001 - CLI returns a deterministic non-zero code
        print(json.dumps({"status": "ERROR", "error": str(exc)}, ensure_ascii=False))
        return 1
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
