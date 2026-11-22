"""Đọc, lưu, đối soát cặp READY report P2/P3 và kiểm tra trực tiếp 6 physical index trên ES."""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from ..config.es_client import ESClient, load_runtime_config, validate_index_name

DEFAULT_BATCH_ALIASES: Tuple[str, ...] = tuple(
    load_runtime_config()["batch_publication_aliases"]
)


@dataclass(frozen=True)
class IndexReadyEntry:
    """Thông số đối soát của từng physical index trong READY report."""

    alias_name: str
    physical_index: str
    row_count: int
    duplicate_id_count: int
    bulk_error_count: int
    sum_count: Optional[float] = None


@dataclass(frozen=True)
class ReadyReport:
    """Báo cáo sẵn sàng do P2 hoặc P3 xuất ra trước khi P5 chuyển 6 Batch aliases."""

    owner: str
    schema_version: str
    manifest_sha256: str
    batch_run_id: str
    batch_cutoff_time: str
    ref_version: str
    sql_test_result: str
    completed_at: str
    status: str
    indices: List[IndexReadyEntry]


def _parse_utc_timestamp(ts: str) -> datetime:
    cleaned = ts.strip()
    if cleaned.endswith("Z"):
        cleaned = cleaned[:-1] + "+00:00"
    dt = datetime.fromisoformat(cleaned)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def save_ready_report(report: ReadyReport, path: Path | str) -> Path:
    """Ghi READY report ra file JSON theo đúng format chuẩn (dùng atomic write)."""
    out_path = Path(path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "owner": report.owner,
        "schema_version": report.schema_version,
        "manifest_sha256": report.manifest_sha256,
        "batch_run_id": report.batch_run_id,
        "batch_cutoff_time": report.batch_cutoff_time,
        "ref_version": report.ref_version,
        "sql_test_result": report.sql_test_result,
        "completed_at": report.completed_at,
        "status": report.status,
        "indices": [asdict(entry) for entry in report.indices],
    }
    tmp_path = out_path.with_name(f".{out_path.name}.tmp.{os.getpid()}")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp_path, out_path)
    return out_path


def load_ready_report(path: Path | str) -> ReadyReport:
    """Đọc và kiểm tra cấu trúc file READY report JSON."""
    report_path = Path(path)
    raw = json.loads(report_path.read_text(encoding="utf-8"))

    required_top = (
        "owner",
        "schema_version",
        "manifest_sha256",
        "batch_run_id",
        "batch_cutoff_time",
        "ref_version",
        "sql_test_result",
        "completed_at",
        "status",
        "indices",
    )
    for key in required_top:
        if key not in raw:
            raise ValueError(f"READY report {report_path} thiếu trường bắt buộc '{key}'")

    if not isinstance(raw["indices"], list) or not raw["indices"]:
        raise ValueError(f"READY report {report_path} 'indices' phải là danh sách không rỗng")

    entries: List[IndexReadyEntry] = []
    for item in raw["indices"]:
        for field_name in (
            "alias_name",
            "physical_index",
            "row_count",
            "duplicate_id_count",
            "bulk_error_count",
        ):
            if field_name not in item:
                raise ValueError(
                    f"READY report {report_path} thiếu '{field_name}' trong entry của indices"
                )
        entries.append(
            IndexReadyEntry(
                alias_name=validate_index_name(str(item["alias_name"])),
                physical_index=validate_index_name(str(item["physical_index"])),
                row_count=int(item["row_count"]),
                duplicate_id_count=int(item["duplicate_id_count"]),
                bulk_error_count=int(item["bulk_error_count"]),
                sum_count=float(item["sum_count"]) if item.get("sum_count") is not None else None,
            )
        )

    return ReadyReport(
        owner=str(raw["owner"]).strip(),
        schema_version=str(raw["schema_version"]).strip(),
        manifest_sha256=str(raw["manifest_sha256"]).strip(),
        batch_run_id=str(raw["batch_run_id"]).strip(),
        batch_cutoff_time=str(raw["batch_cutoff_time"]).strip(),
        ref_version=str(raw["ref_version"]).strip(),
        sql_test_result=str(raw["sql_test_result"]).strip(),
        completed_at=str(raw["completed_at"]).strip(),
        status=str(raw["status"]).strip(),
        indices=entries,
    )


def validate_ready_reports_pair(
    p2_report: ReadyReport,
    p3_report: ReadyReport,
    expected_aliases: Sequence[str] = DEFAULT_BATCH_ALIASES,
) -> Dict[str, IndexReadyEntry]:
    """Đối soát P2 và P3 cùng run_id, T, ref_version, manifest_sha256 và đủ 6 bảng Batch."""
    expected_schema_version = str(load_runtime_config()["schema_version"])
    for label, report in (("P2", p2_report), ("P3", p3_report)):
        if report.status != "READY":
            raise ValueError(f"Báo cáo {label} phải có status='READY', nhận được {report.status!r}")
        if report.sql_test_result != "PASS":
            raise ValueError(
                f"Báo cáo {label} phải có sql_test_result='PASS', nhận được {report.sql_test_result!r}"
            )
        if report.schema_version != expected_schema_version:
            raise ValueError(
                f"Báo cáo {label} dùng schema_version={report.schema_version!r}; "
                f"runtime yêu cầu {expected_schema_version!r}"
            )
        for attr in (
            "owner",
            "schema_version",
            "manifest_sha256",
            "batch_run_id",
            "batch_cutoff_time",
            "ref_version",
        ):
            if not getattr(report, attr):
                raise ValueError(f"Báo cáo {label} có trường '{attr}' bị rỗng")
        _parse_utc_timestamp(report.batch_cutoff_time)

    for attr in ("schema_version", "manifest_sha256", "batch_run_id", "ref_version"):
        v2 = getattr(p2_report, attr)
        v3 = getattr(p3_report, attr)
        if v2 != v3:
            raise ValueError(f"Lệch metadata '{attr}' giữa P2 và P3: {v2!r} != {v3!r}")

    if _parse_utc_timestamp(p2_report.batch_cutoff_time) != _parse_utc_timestamp(
        p3_report.batch_cutoff_time
    ):
        raise ValueError(
            "Lệch 'batch_cutoff_time' giữa P2 và P3: "
            f"{p2_report.batch_cutoff_time!r} != {p3_report.batch_cutoff_time!r}"
        )

    expected_set = {validate_index_name(a) for a in expected_aliases}
    by_alias: Dict[str, IndexReadyEntry] = {}

    for report in (p2_report, p3_report):
        for entry in report.indices:
            alias = validate_index_name(entry.alias_name)
            physical = validate_index_name(entry.physical_index)
            if alias == physical:
                raise ValueError(
                    f"Tên physical index phải gắn phiên bản và khác tên alias {alias!r}"
                )
            if entry.row_count <= 0:
                raise ValueError(
                    f"Physical index {physical!r} có row_count={entry.row_count} không hợp lệ"
                )
            if entry.bulk_error_count != 0:
                raise ValueError(
                    f"Physical index {physical!r} còn bulk_error_count={entry.bulk_error_count}"
                )
            if entry.duplicate_id_count != 0:
                raise ValueError(
                    f"Physical index {physical!r} còn duplicate_id_count={entry.duplicate_id_count}"
                )
            if alias in by_alias:
                raise ValueError(f"Bị trùng khai báo alias {alias!r} giữa các READY report")
            by_alias[alias] = entry

    missing = sorted(expected_set - set(by_alias.keys()))
    extra = sorted(set(by_alias.keys()) - expected_set)
    if missing:
        raise ValueError(f"Thiếu Batch alias trong cặp READY report P2/P3: {missing}")
    if extra:
        raise ValueError(f"Có alias lạ ngoài danh sách Batch trong READY report: {extra}")

    return by_alias


def verify_physical_indices_in_es(
    client: ESClient,
    p2_report: ReadyReport,
    p3_report: ReadyReport,
    expected_aliases: Sequence[str] = DEFAULT_BATCH_ALIASES,
) -> Dict[str, IndexReadyEntry]:
    """Query thẳng Elasticsearch để xác nhận cả 6 physical index tồn tại, đúng count và 1 run duy nhất."""
    by_alias = validate_ready_reports_pair(
        p2_report, p3_report, expected_aliases=expected_aliases
    )
    expected_run_id = p2_report.batch_run_id
    expected_cutoff = _parse_utc_timestamp(p2_report.batch_cutoff_time)

    for alias_name, entry in by_alias.items():
        physical = entry.physical_index
        if not client.index_exists(physical):
            raise ValueError(
                f"Physical index {physical!r} (cho alias {alias_name!r}) chưa tồn tại trên ES"
            )
        actual_count = client.count(physical)
        if actual_count != entry.row_count:
            raise ValueError(
                f"Lệch số dòng trên ES cho {physical!r}: "
                f"báo cáo {entry.row_count}, thực tế {actual_count}"
            )

        agg_resp = client.search(
            physical,
            {
                "size": 0,
                "aggs": {
                    "runs": {"terms": {"field": "batch_run_id", "size": 10}},
                    "cutoffs": {"terms": {"field": "batch_cutoff_time", "size": 10}},
                },
            },
        )
        aggs = agg_resp.get("aggregations", {})

        run_buckets = aggs.get("runs", {}).get("buckets", [])
        if (
            len(run_buckets) != 1
            or str(run_buckets[0].get("key")) != expected_run_id
            or int(run_buckets[0].get("doc_count", -1)) != actual_count
        ):
            raise ValueError(
                f"Phát hiện trộn/lệch batch_run_id trong {physical!r}: "
                f"kỳ vọng duy nhất {expected_run_id!r} ({actual_count} docs), nhận {run_buckets!r}"
            )

        cutoff_buckets = aggs.get("cutoffs", {}).get("buckets", [])
        if len(cutoff_buckets) != 1 or int(cutoff_buckets[0].get("doc_count", -1)) != actual_count:
            raise ValueError(
                f"Phát hiện trộn/lệch batch_cutoff_time trong {physical!r}: nhận {cutoff_buckets!r}"
            )
        bucket = cutoff_buckets[0]
        if "key_as_string" in bucket:
            actual_cutoff = _parse_utc_timestamp(str(bucket["key_as_string"]))
        else:
            actual_cutoff = datetime.fromtimestamp(float(bucket["key"]) / 1000.0, tz=timezone.utc)
        if actual_cutoff != expected_cutoff:
            raise ValueError(
                f"Lệch batch_cutoff_time trong {physical!r}: "
                f"kỳ vọng {expected_cutoff.isoformat()}, thực tế {actual_cutoff.isoformat()}"
            )

    return by_alias
