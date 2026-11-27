"""Loader dữ liệu thật genre_daily_stats.tsv (Module 04) lên physical source index genre_trends_<run_id> (P3 - K1).

Đầu vào: file TSV (hoặc thư mục chứa part-*) 9 cột không header theo DATA_CONTRACT.md:
  date, genre, interaction_count, rating_count, rating_sum, avg_rating, batch_run_id, batch_cutoff_time, generated_at
và file manifest.json đi kèm.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import date
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

from modules.elasticsearch.common.config.es_client import ESClient, validate_index_name
from modules.elasticsearch.lanes.genre_batch.load_fixtures import check_template
from modules.spark_sql.common.writers.key_serializer import build_record_id

GENRE_KEY_FIELDS = ("batch_run_id", "date", "genre")


def _iter_tsv_files(input_path: Path) -> List[Path]:
    if input_path.is_file():
        return [input_path]
    if input_path.is_dir():
        files = sorted(p for p in input_path.rglob("*") if p.is_file() and not p.name.startswith("_") and not p.name.startswith("."))
        if not files:
            raise FileNotFoundError(f"Không tìm thấy file part trong thư mục: {input_path}")
        return files
    raise FileNotFoundError(f"Không tồn tại đường dẫn đầu vào: {input_path}")


def load_manifest_info(manifest_path: Path) -> Tuple[dict, str]:
    raw_bytes = manifest_path.read_bytes()
    manifest_sha256 = hashlib.sha256(raw_bytes).hexdigest()
    manifest = json.loads(raw_bytes.decode("utf-8"))
    for key in ("run_id", "batch_cutoff_time", "reference"):
        if key not in manifest:
            raise ValueError(f"Manifest {manifest_path} thiếu trường bắt buộc '{key}'")
    if not manifest["reference"].get("ref_version"):
        raise ValueError(f"Manifest {manifest_path} thiếu reference.ref_version")
    return manifest, manifest_sha256


def parse_genre_daily_tsv(
    input_path: Path,
    *,
    expected_run_id: str,
    expected_cutoff: str,
) -> List[Dict[str, Any]]:
    """Đọc, kiểm tra contract và gắn _id xác định cho từng dòng genre_daily_stats.tsv."""
    cutoff_date = expected_cutoff[:10]
    records: List[Dict[str, Any]] = []
    seen_ids: set[str] = set()

    for tsv_file in _iter_tsv_files(input_path):
        for line_no, raw_line in enumerate(tsv_file.read_text(encoding="utf-8").splitlines(), start=1):
            if not raw_line.strip():
                continue
            cols = raw_line.split("\t")
            if len(cols) != 9:
                raise ValueError(f"{tsv_file}:{line_no}: kỳ vọng 9 cột TSV, nhận {len(cols)}")
            row_date, genre, inter_str, rating_str, sum_str, avg_str, run_id, cutoff, generated_at = cols

            if not genre:
                raise ValueError(f"{tsv_file}:{line_no}: genre bị rỗng")
            parsed_date = date.fromisoformat(row_date)
            if row_date >= cutoff_date:
                raise ValueError(f"{tsv_file}:{line_no}: date={row_date} không nhỏ hơn cutoff {cutoff_date}")
            if run_id != expected_run_id or cutoff != expected_cutoff:
                raise ValueError(
                    f"{tsv_file}:{line_no}: metadata ({run_id}, {cutoff}) lệch manifest ({expected_run_id}, {expected_cutoff})"
                )

            interaction_count = int(inter_str)
            rating_count = int(rating_str)
            rating_sum = float(sum_str)
            avg_rating = float(avg_str)

            if interaction_count < 1 or rating_count < 1 or interaction_count != rating_count:
                raise ValueError(f"{tsv_file}:{line_no}: interaction_count={interaction_count} != rating_count={rating_count}")
            if rating_sum < 0.5 or not (0.5 <= avg_rating <= 5.0):
                raise ValueError(f"{tsv_file}:{line_no}: rating_sum/avg_rating ngoài khoảng hợp lệ")
            if abs(avg_rating - (rating_sum / rating_count)) >= 1e-6:
                raise ValueError(f"{tsv_file}:{line_no}: avg_rating={avg_rating} không khớp rating_sum/rating_count")

            record_id = build_record_id(
                {"batch_run_id": run_id, "date": parsed_date, "genre": genre},
                GENRE_KEY_FIELDS,
            )
            if record_id in seen_ids:
                raise ValueError(f"{tsv_file}:{line_no}: trùng khóa ({run_id}, {row_date}, {genre})")
            seen_ids.add(record_id)

            records.append(
                {
                    "_id": record_id,
                    "batch_run_id": run_id,
                    "batch_cutoff_time": cutoff,
                    "generated_at": generated_at,
                    "date": row_date,
                    "genre": genre,
                    "interaction_count": interaction_count,
                    "rating_count": rating_count,
                    "rating_sum": rating_sum,
                    "avg_rating": avg_rating,
                }
            )
    if not records:
        raise ValueError(f"Không có bản ghi nào trong {input_path}")
    return records


def load_genre_source(
    input_path: Path,
    manifest_path: Path,
    *,
    target_index: str | None = None,
    dry_run: bool = False,
    batch_size: int = 1000,
) -> Dict[str, Any]:
    manifest, manifest_sha256 = load_manifest_info(manifest_path)
    run_id = str(manifest["run_id"])
    cutoff = str(manifest["batch_cutoff_time"])
    ref_version = str(manifest["reference"]["ref_version"])

    resolved_index = validate_index_name(target_index or f"genre_trends_{run_id.lower()}")
    if resolved_index == "genre_trends":
        raise ValueError("Không được ghi trực tiếp vào canonical alias 'genre_trends'")

    records = parse_genre_daily_tsv(input_path, expected_run_id=run_id, expected_cutoff=cutoff)
    total_assignments = sum(r["interaction_count"] for r in records)

    summary: Dict[str, Any] = {
        "target_index": resolved_index,
        "batch_run_id": run_id,
        "batch_cutoff_time": cutoff,
        "ref_version": ref_version,
        "manifest_sha256": manifest_sha256,
        "row_count": len(records),
        "sum_count": float(total_assignments),
        "unknown_rows": sum(1 for r in records if r["genre"] == "UNKNOWN"),
        "dry_run": dry_run,
    }
    if dry_run:
        return summary

    client = ESClient(owner="p3")
    check_template(client, resolved_index, {k for r in records for k in r if k != "_id"})
    result = client.bulk_upsert(
        resolved_index,
        records,
        id_field="_id",
        batch_size=batch_size,
        refresh=True,
        raise_on_error=True,
    )
    es_count = client.count(resolved_index)
    if es_count != len(records):
        raise RuntimeError(f"{resolved_index}: ES có {es_count} document, nguồn có {len(records)} dòng")
    summary["succeeded_count"] = result.succeeded_count
    summary["failed_count"] = result.failed_count
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Nạp genre_daily_stats.tsv lên physical index ES (P3 - K1)")
    parser.add_argument("--input", type=Path, required=True, help="Đường dẫn file/thư mục genre_daily_stats.tsv")
    parser.add_argument("--manifest", type=Path, required=True, help="Đường dẫn file manifest.json của batch run")
    parser.add_argument("--target-index", default=None, help="Physical index đích (mặc định genre_trends_<run_id>)")
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--dry-run", action="store_true", help="Chỉ validate TSV/manifest và in summary, không ghi ES")
    args = parser.parse_args()

    summary = load_genre_source(
        args.input,
        args.manifest,
        target_index=args.target_index,
        dry_run=args.dry_run,
        batch_size=args.batch_size,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
