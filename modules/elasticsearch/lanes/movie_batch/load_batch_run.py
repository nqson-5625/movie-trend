"""Nạp Batch View thật của Module 04 vào hai physical source index của một run (Phase2 K1, việc P2.2).

Đầu vào là thư mục run đã tải về bằng `make -C modules/mapreduce download`:
    data/local/mapreduce/export/run_id=<run_id>/{manifest.json, movie_stats.tsv, movie_daily_stats.tsv}
Ghi vào <prefix>movie_batch_stats_<run_id> và <prefix>movie_daily_stats_<run_id> (run_id viết thường).
Alias chính do P5 publish sau READY; script này không đụng alias.

    .venv/bin/python -m modules.elasticsearch.lanes.movie_batch.load_batch_run --dry-run   # chỉ đọc + kiểm tra
    .venv/bin/python -m modules.elasticsearch.lanes.movie_batch.load_batch_run             # nạp run chính thức
    .venv/bin/python -m modules.elasticsearch.lanes.movie_batch.load_batch_run --prefix dev_p2_
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker

from modules.elasticsearch.common.config.es_client import ESClient
from modules.elasticsearch.lanes.movie_batch.load_fixtures import build_records, check_template

PROJECT_ROOT = Path(__file__).resolve().parents[4]
DEFAULT_RUN_DIR = PROJECT_ROOT / "data/local/mapreduce/export/run_id=20261024T104145Z"
SCHEMAS_DIR = PROJECT_ROOT / "contracts/phase2/movie_batch/schemas"
REPORTS_DIR = PROJECT_ROOT / "data/local/phase2/es_reports"

# alias source -> (tên job/file TSV của Module 04, cột đếm dùng để đối soát tổng)
SOURCES = {
    "movie_batch_stats": ("movie_stats", "total_count"),
    "movie_daily_stats": ("movie_daily_stats", "interaction_count"),
}
INT_FIELDS = {"movieId", "total_count", "rating_count", "interaction_count"}
FLOAT_FIELDS = {"rating_sum", "avg_rating"}


@dataclass
class BatchRun:
    run_id: str
    batch_cutoff_time: str
    generated_at: str
    ref_version: str
    manifest_sha256: str
    docs: dict[str, list[dict]] = field(default_factory=dict)


def physical_index(family: str, run_id: str, prefix: str = "") -> str:
    return f"{prefix}{family}_{run_id.lower()}"


def parse_tsv(path: Path, columns: list[str]) -> list[dict]:
    """TSV của Module 04 (có hoặc không có dòng header), đổi kiểu theo schema; title rỗng -> null."""
    docs = []
    with path.open(encoding="utf-8", newline="") as f:
        for line_no, row in enumerate(csv.reader(f, delimiter="\t", quoting=csv.QUOTE_NONE), start=1):
            if line_no == 1 and row == columns:
                continue
            if len(row) != len(columns):
                raise ValueError(f"{path.name}:{line_no}: có {len(row)} cột, manifest ghi {len(columns)}")
            doc = {}
            for name, raw in zip(columns, row):
                if name in INT_FIELDS:
                    doc[name] = int(raw)
                elif name in FLOAT_FIELDS:
                    doc[name] = float(raw)
                elif name == "title":
                    doc[name] = raw or None
                else:
                    doc[name] = raw
            docs.append(doc)
    return docs


def read_run(run_dir: Path) -> BatchRun:
    manifest_bytes = (run_dir / "manifest.json").read_bytes()
    manifest = json.loads(manifest_bytes)
    run = BatchRun(
        run_id=manifest["run_id"],
        batch_cutoff_time=manifest["batch_cutoff_time"],
        generated_at=manifest["generated_at"],
        ref_version=manifest["reference"]["ref_version"],
        manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest(),
    )
    for family, (job, _) in SOURCES.items():
        spec = manifest["jobs"][job]
        docs = parse_tsv(run_dir / f"{job}.tsv", spec["columns"])
        expected = spec["counters"]["framework"]["Reduce output records"]
        if len(docs) != expected:
            raise ValueError(f"{job}.tsv có {len(docs)} dòng, manifest ghi Reduce output records = {expected}")
        run.docs[family] = docs
    validate_run(run)
    return run


def validate_run(run: BatchRun) -> None:
    """Schema từng dòng + đối soát chéo hai bảng; lỗi đầu tiên là dừng, không ghi gì lên ES."""
    meta = {"batch_run_id": run.run_id, "batch_cutoff_time": run.batch_cutoff_time, "generated_at": run.generated_at}
    for family, docs in run.docs.items():
        validator = Draft202012Validator(json.loads((SCHEMAS_DIR / f"{family}.schema.json").read_text(encoding="utf-8")),
                                         format_checker=FormatChecker())
        for i, doc in enumerate(docs, start=1):
            error = next(validator.iter_errors(doc), None)
            if error is not None:
                raise ValueError(f"{family} dòng {i}: {error.message}")
            if any(doc[k] != v for k, v in meta.items()):
                raise ValueError(f"{family} dòng {i}: batch_run_id/batch_cutoff_time/generated_at khác manifest")
            count = doc.get("total_count", doc.get("interaction_count"))
            if count != doc["rating_count"] or not math.isclose(doc["avg_rating"], doc["rating_sum"] / count):
                raise ValueError(f"{family} dòng {i}: count/sum/avg không nhất quán")

    movies = {d["movieId"]: d for d in run.docs["movie_batch_stats"]}
    if len(movies) != len(run.docs["movie_batch_stats"]):
        raise ValueError("movie_batch_stats trùng movieId")
    per_movie: dict[int, list] = defaultdict(lambda: [0, 0.0])
    seen = set()
    for d in run.docs["movie_daily_stats"]:
        if (d["date"], d["movieId"]) in seen:
            raise ValueError(f"movie_daily_stats trùng ({d['date']}, {d['movieId']})")
        seen.add((d["date"], d["movieId"]))
        cutoff_day, at_midnight = run.batch_cutoff_time[:10], run.batch_cutoff_time[10:] == "T00:00:00Z"
        if d["date"] > cutoff_day or (d["date"] == cutoff_day and at_midnight):
            raise ValueError(f"movie_daily_stats có ngày {d['date']} không trước mốc T")
        per_movie[d["movieId"]][0] += d["interaction_count"]
        per_movie[d["movieId"]][1] += d["rating_sum"]
    if set(per_movie) != set(movies):
        raise ValueError("Tập phim của movie_daily_stats khác movie_batch_stats")
    for movie, (count, total) in per_movie.items():
        if count != movies[movie]["total_count"] or not math.isclose(total, movies[movie]["rating_sum"]):
            raise ValueError(f"Phim {movie}: cộng theo ngày ({count}, {total}) khác movie_batch_stats")


def load_run(run: BatchRun, client: ESClient, prefix: str = "") -> list[dict]:
    entries = []
    for family, (_, count_field) in SOURCES.items():
        index = physical_index(family, run.run_id, prefix)
        records = build_records(family, run.docs[family])
        check_template(client, index, {k for r in records for k in r if k != "_id"})
        result = client.bulk_upsert(index, records, batch_size=2000)
        client.refresh(index)
        expected_sum = sum(d[count_field] for d in run.docs[family])
        resp = client.search(index, {"size": 0, "track_total_hits": True,
                                     "aggs": {"s": {"sum": {"field": count_field}}}})
        count, total = resp["hits"]["total"]["value"], int(resp["aggregations"]["s"]["value"])
        if (count, total) != (len(records), expected_sum):
            raise RuntimeError(f"{index}: ES có {count} dòng / tổng {total}, kỳ vọng {len(records)} / {expected_sum}")
        print(f"[OK] {index}: {count} document, tổng {count_field} = {total}")
        entries.append({"alias_name": family, "physical_index": index, "row_count": count,
                        "duplicate_id_count": 0, "bulk_error_count": result.failed_count, "sum_count": float(total)})
    return entries


def save_report(run: BatchRun, entries: list[dict], prefix: str) -> Path:
    """Số liệu nạp source, dùng lại khi lập READY report P2 (cùng các field ReadyReport)."""
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    path = REPORTS_DIR / f"{prefix}p2_source_{run.run_id}.json"
    report = {"owner": "p2", "batch_run_id": run.run_id, "batch_cutoff_time": run.batch_cutoff_time,
              "ref_version": run.ref_version, "manifest_sha256": run.manifest_sha256,
              "loaded_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), "indices": entries}
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def main() -> int:
    parser = argparse.ArgumentParser(description="Nạp movie_stats/movie_daily_stats của một batch run vào ES")
    parser.add_argument("--run-dir", type=Path, default=DEFAULT_RUN_DIR, help="thư mục export của run")
    parser.add_argument("--prefix", default="", choices=["", "dev_p2_"], help="'' = physical thật, dev_p2_ = thử")
    parser.add_argument("--dry-run", action="store_true", help="chỉ đọc và kiểm tra, không ghi ES")
    args = parser.parse_args()

    run = read_run(args.run_dir)
    for family, docs in run.docs.items():
        print(f"[OK] {family}: {len(docs)} dòng hợp lệ")
    print(f"     run_id={run.run_id} T={run.batch_cutoff_time} ref_version={run.ref_version}")
    if args.dry_run:
        return 0
    entries = load_run(run, ESClient(owner="p2"), args.prefix)
    print(f"[OK] Báo cáo: {save_report(run, entries, args.prefix)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
