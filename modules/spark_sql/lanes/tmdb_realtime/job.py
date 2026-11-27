"""Sequential Spark SQL sync & versioned publish job for `tmdb_realtime` -> `analytics_tmdb_signal`."""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import re
import signal
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from modules.elasticsearch.common.config.es_client import (
    ESClient,
    load_runtime_config,
    validate_index_name,
)
from modules.elasticsearch.common.publication.versioned_publisher import (
    DEFAULT_REPORT_DIR,
    VersionedReadyReport,
    publish_versioned_alias,
    save_versioned_ready_report,
)
from modules.spark_sql.common.readers.es_reader import register_temp_view
from modules.spark_sql.common.runtime.spark_session import create_spark_sql_session
from modules.spark_sql.common.writers.analytics_writer import (
    ANALYTICS_KEY_COLUMNS,
    validate_analytics_dataframe,
    validate_physical_target,
    write_analytics_df,
    write_analytics_df_distributed,
)

from .change_detector import DEFAULT_TOP_K, ChangeDetectionResult, detect_top_k_change

SQL_PATH = Path(__file__).resolve().parent / "sql" / "analytics_tmdb_signal.sql"
DEFAULT_SOURCE_INDEX = "tmdb_realtime"
DEFAULT_ALIAS = "analytics_tmdb_signal"
REQUIRED_MAPPING_FIELDS = (
    "schema_version",
    "analytics_run_id",
    "published_at",
    "tmdbId",
    "event_time",
    "popularity",
    "vote_count",
    "vote_average",
    "updated_at",
    "source",
)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _atomic_write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp, path)


@contextmanager
def _file_lock(lock_path: Path) -> Iterator[None]:
    """Serialize sequential execution across processes using an exclusive advisory file lock."""
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with open(lock_path, "a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def generate_version_tag(now: Optional[datetime] = None) -> str:
    """Return lowercase UTC millisecond timestamp tag (`YYYYMMDDtHHMMSSmmmz`)."""
    dt = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    millis = dt.microsecond // 1000
    return f"{dt.strftime('%Y%m%dt%H%M%S')}{millis:03d}z"


def load_sql_query(top_k: int = DEFAULT_TOP_K, sql_path: Path = SQL_PATH) -> str:
    """Load the lane SQL query and apply `top_k` if overridden from default 20."""
    if top_k <= 0:
        raise ValueError(f"top_k phải lớn hơn 0, nhận {top_k}")
    sql_text = sql_path.read_text(encoding="utf-8").strip()
    if top_k != 20:
        updated, count = re.subn(
            r"\bLIMIT\s+\d+\s*;?\s*$",
            f"LIMIT {int(top_k)}",
            sql_text,
            flags=re.IGNORECASE,
        )
        if count != 1:
            raise ValueError(f"Không tìm thấy mệnh đề LIMIT cuối câu SQL trong {sql_path}")
        sql_text = updated
    return sql_text


def build_tmdb_signal_df(
    spark: SparkSession,
    *,
    schema_version: str,
    analytics_run_id: str,
    published_at: str,
    top_k: int = DEFAULT_TOP_K,
    sql_path: Path = SQL_PATH,
) -> DataFrame:
    """Execute the TMDB signal SQL against `tmdb_rt` and attach common analytics metadata."""
    sql_text = load_sql_query(top_k=top_k, sql_path=sql_path)
    df = (
        spark.sql(sql_text)
        .withColumn("schema_version", F.lit(schema_version))
        .withColumn("analytics_run_id", F.lit(analytics_run_id))
        .withColumn("published_at", F.to_timestamp(F.lit(published_at)))
    )
    validate_analytics_dataframe(df, "analytics_tmdb_signal")
    return df


def cleanup_old_physical_indices(
    admin_client: ESClient,
    *,
    alias_name: str = DEFAULT_ALIAS,
    keep_versions: int = 5,
) -> List[str]:
    """Delete older unaliased `analytics_tmdb_signal-v*` physical indices, keeping `keep_versions`."""
    if keep_versions < 1:
        raise ValueError("keep_versions phải >= 1")
    clean_alias = validate_index_name(alias_name)
    prefix = f"{clean_alias}-v"

    all_aliases = admin_client.get_aliases()
    matching = [idx for idx in all_aliases if idx.startswith(prefix)]
    if not matching:
        return []

    aliased = {
        idx
        for idx, meta in all_aliases.items()
        if isinstance(meta, dict) and bool(meta.get("aliases"))
    }
    ordered_desc = sorted(matching, reverse=True)
    keep = set(ordered_desc[:keep_versions]) | (set(matching) & aliased)

    deleted: List[str] = []
    for idx in sorted(set(matching) - keep):
        admin_client.delete_index(idx, ignore_missing=True)
        deleted.append(idx)
    return deleted


def _allocate_unique_version(
    writer_client: ESClient,
    alias_name: str,
) -> tuple[str, str]:
    """Generate a monotonically sortable version tag not already present in ES."""
    for _ in range(10):
        version = generate_version_tag()
        physical_index = validate_physical_target(
            f"{alias_name}-v{version}", "analytics_tmdb_signal"
        )
        if not writer_client.index_exists(physical_index):
            return version, physical_index
        time.sleep(0.002)
    raise RuntimeError(f"Không thể cấp phát version index mới duy nhất cho {alias_name}")


def _write_sync_state(report_dir: Path | str, payload: Dict[str, Any]) -> Path:
    state_path = Path(report_dir) / "tmdb_signal_sync_state.json"
    _atomic_write_json(state_path, payload)
    return state_path


def _detection_dict(detection: Optional[ChangeDetectionResult]) -> Dict[str, Any]:
    if detection is None:
        return {
            "reason": "FORCED",
            "source_count": None,
            "target_count": None,
            "source_top_ids": None,
            "target_top_ids": None,
        }
    return {
        "reason": detection.reason,
        "source_count": detection.source_count,
        "target_count": detection.target_count,
        "source_top_ids": list(detection.source_top_ids),
        "target_top_ids": list(detection.target_top_ids),
    }


def sync_once(
    *,
    source_index: str = DEFAULT_SOURCE_INDEX,
    alias_name: str = DEFAULT_ALIAS,
    top_k: int = DEFAULT_TOP_K,
    io_mode: str = "fixture",
    report_dir: Path | str = DEFAULT_REPORT_DIR,
    dry_run: bool = False,
    force: bool = False,
    keep_versions: Optional[int] = 5,
    spark: Optional[SparkSession] = None,
    writer_client: Optional[ESClient] = None,
    admin_client: Optional[ESClient] = None,
) -> Dict[str, Any]:
    """Run one sequential change-check -> Spark SQL -> physical write -> atomic swap cycle."""
    clean_source = validate_index_name(source_index)
    clean_alias = validate_index_name(alias_name)
    if io_mode not in {"fixture", "distributed"}:
        raise ValueError("io_mode phải là 'fixture' hoặc 'distributed'")
    if keep_versions is not None and keep_versions < 1:
        raise ValueError("keep_versions phải >= 1")

    report_root = Path(report_dir)
    writer = writer_client or ESClient(owner="p5")

    with _file_lock(report_root / ".tmdb_signal_sync.lock"):
        detection: Optional[ChangeDetectionResult] = None
        if not force:
            detection = detect_top_k_change(
                writer,
                source_index=clean_source,
                target_alias=clean_alias,
                top_k=top_k,
            )
            if not detection.has_change:
                result: Dict[str, Any] = {
                    "status": "NO_CHANGE",
                    "checked_at": _utc_now_iso(),
                    "source_index": clean_source,
                    "alias_name": clean_alias,
                    **_detection_dict(detection),
                }
                _write_sync_state(report_root, result)
                return result

        cfg = load_runtime_config()
        version, physical_index = _allocate_unique_version(writer, clean_alias)
        analytics_run_id = f"tmdb-{version}"
        published_at = _utc_now_iso()

        owns_spark = spark is None
        session = spark or create_spark_sql_session(app_name="p5-analytics-tmdb-signal")
        try:
            register_temp_view(
                session,
                "tmdb_rt",
                index_name=clean_source,
                client=writer,
                mode=io_mode,
            )
            output_df = build_tmdb_signal_df(
                session,
                schema_version=str(cfg["schema_version"]),
                analytics_run_id=analytics_run_id,
                published_at=published_at,
                top_k=top_k,
            )

            if io_mode == "distributed":
                if output_df.limit(1).count() == 0:
                    raise ValueError(
                        f"Không có bản ghi nào từ '{clean_source}' để build {clean_alias}"
                    )
                writer.create_index(physical_index, require_absent=True)
                dist_res = write_analytics_df_distributed(
                    output_df,
                    physical_index,
                    "analytics_tmdb_signal",
                )
                writer.refresh(physical_index)
                row_count = dist_res.written_count
            else:
                if output_df.limit(1).count() == 0:
                    raise ValueError(
                        f"Không có bản ghi nào từ '{clean_source}' để build {clean_alias}"
                    )
                writer.create_index(physical_index, require_absent=True)
                bulk_res = write_analytics_df(
                    output_df,
                    physical_index,
                    ANALYTICS_KEY_COLUMNS["analytics_tmdb_signal"],
                    owner="p5",
                    client=writer,
                    refresh=True,
                    analytics_family="analytics_tmdb_signal",
                )
                row_count = bulk_res.succeeded_count
        finally:
            if owns_spark:
                session.stop()

        ready_report = VersionedReadyReport(
            alias_name=clean_alias,
            physical_index=physical_index,
            version=version,
            analytics_run_id=analytics_run_id,
            schema_version=str(cfg["schema_version"]),
            row_count=row_count,
            bulk_error_count=0,
            duplicate_key_count=0,
            completed_at=_utc_now_iso(),
            status="READY",
            required_mapping_fields=REQUIRED_MAPPING_FIELDS,
        )
        ready_path = report_root / f"tmdb_signal_ready_{version}.json"
        save_versioned_ready_report(ready_report, ready_path)

        admin = admin_client or ESClient(owner="admin")
        publish_res = publish_versioned_alias(
            admin,
            ready_report,
            report_dir=report_root,
            dry_run=dry_run,
        )

        deleted_old: List[str] = []
        if not dry_run and keep_versions is not None:
            deleted_old = cleanup_old_physical_indices(
                admin,
                alias_name=clean_alias,
                keep_versions=keep_versions,
            )

        summary: Dict[str, Any] = {
            "status": publish_res["status"],
            "checked_at": _utc_now_iso(),
            "source_index": clean_source,
            "alias_name": clean_alias,
            "physical_index": physical_index,
            "version": version,
            "analytics_run_id": analytics_run_id,
            "published_at": published_at,
            "row_count": row_count,
            "ready_report": str(ready_path),
            "deleted_old_indices": deleted_old,
            **_detection_dict(detection),
        }
        _write_sync_state(report_root, summary)
        return summary


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Sequential Spark SQL sync & atomic alias swap for analytics_tmdb_signal"
    )
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument("--once", action="store_true", help="Run one sync check (default)")
    mode_group.add_argument("--loop", action="store_true", help="Poll sequentially in a loop")
    parser.add_argument("--poll-interval", type=float, default=15.0)
    parser.add_argument("--keep-spark-alive", action="store_true")
    parser.add_argument("--source-index", default=DEFAULT_SOURCE_INDEX)
    parser.add_argument("--alias", default=DEFAULT_ALIAS)
    parser.add_argument("--top-k", type=int, default=DEFAULT_TOP_K)
    parser.add_argument("--io-mode", choices=["fixture", "distributed"], default="fixture")
    parser.add_argument("--report-dir", default=str(DEFAULT_REPORT_DIR))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--keep-versions", type=int, default=5)
    args = parser.parse_args(argv)

    if args.poll_interval <= 0:
        print(json.dumps({"status": "ERROR", "error": "--poll-interval phải > 0"}), file=sys.stderr)
        return 1

    stop_requested = False

    def _handle_stop(signum: int, frame: Any) -> None:  # noqa: ARG001
        nonlocal stop_requested
        stop_requested = True

    old_sigint = signal.getsignal(signal.SIGINT)
    old_sigterm = signal.getsignal(signal.SIGTERM)
    signal.signal(signal.SIGINT, _handle_stop)
    signal.signal(signal.SIGTERM, _handle_stop)

    shared_spark: Optional[SparkSession] = None
    try:
        if args.loop and args.keep_spark_alive:
            shared_spark = create_spark_sql_session(app_name="p5-analytics-tmdb-signal")

        while True:
            result = sync_once(
                source_index=args.source_index,
                alias_name=args.alias,
                top_k=args.top_k,
                io_mode=args.io_mode,
                report_dir=Path(args.report_dir),
                dry_run=args.dry_run,
                force=args.force,
                keep_versions=args.keep_versions,
                spark=shared_spark,
            )
            print(json.dumps(result, ensure_ascii=False), flush=True)
            if not args.loop or stop_requested:
                break
            deadline = time.monotonic() + args.poll_interval
            while not stop_requested and time.monotonic() < deadline:
                time.sleep(min(0.2, max(0.0, deadline - time.monotonic())))
        return 0
    except Exception as exc:  # noqa: BLE001 - deterministic JSON error exit
        print(json.dumps({"status": "ERROR", "error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 1
    finally:
        if shared_spark is not None:
            shared_spark.stop()
        signal.signal(signal.SIGINT, old_sigint)
        signal.signal(signal.SIGTERM, old_sigterm)


if __name__ == "__main__":
    raise SystemExit(main())
