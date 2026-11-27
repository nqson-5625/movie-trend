"""Sinh fixture lane genre_batch (P3) cho Phase2-3.0 từ bảng ngày viết tay.

Nguồn sự thật duy nhất là DAILY bên dưới; hai bảng được tính từ đó nên luôn khớp nhau:
  genre_trends (source) và analytics_genre_trends (analytics).
Fixture phủ đầy đủ các ca contract mục 9 yêu cầu cho P3:
  - Tối thiểu >= 2 thể loại và >= 2 kỳ (4 thể loại: Action, Comedy, Drama, UNKNOWN; 4 ngày / 2 tháng / 2 năm).
  - Giữ thể loại UNKNOWN.
  - Phim đa thể loại (multi-label): phim gắn cả Action và Drama nên tổng lượt gắn genre (16) > tổng rating thực tế (12).
  - Ca hòa hạng (tie-break): Action và Drama cùng 3 lượt trong 2017-12 -> Action xếp hạng 1, Drama hạng 2.
  - Trung bình có trọng số (weighted avg): Action có 2 ngày với count=1 (avg=4.0) và count=2 (avg=5.0) -> tổng 14.0/3 != (4+5)/2.
  - Kỳ trống (2018-02) không có rating trước mốc T = 2018-03-01T00:00:00Z.

    .venv/bin/python contracts/phase2/genre_batch/build_fixtures.py          # ghi lại fixtures/
    .venv/bin/python contracts/phase2/genre_batch/build_fixtures.py --check  # chỉ kiểm tra fixtures/ còn đúng
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import defaultdict
from pathlib import Path

LANE_DIR = Path(__file__).resolve().parent
FIXTURES_DIR = LANE_DIR / "fixtures"

SCHEMA_VERSION = "phase2-3.0"
BATCH_RUN_ID = "fixture-p3-v1"
BATCH_CUTOFF_TIME = "2018-03-01T00:00:00Z"
GENERATED_AT = "2026-11-27T00:00:00Z"
REF_VERSION = "fixture-ref-v1"
ANALYTICS_RUN_ID = "fixture-p3-v1-analytics"
PUBLISHED_AT = "2026-11-27T00:00:00Z"

# (date, genre, rating_count, rating_sum) — mô phỏng từ genre_daily_stats.tsv
# Giả lập 12 rating thực tế (44.0 tổng điểm), trong đó có 4 rating thuộc phim đa thể loại (Action|Drama)
# -> Tổng lượt gắn genre = 16 lượt (Action: 6, Drama: 5, Comedy: 3, UNKNOWN: 2).
DAILY = [
    ("2017-12-30", "Action", 1, 4.0),
    ("2017-12-30", "Drama", 2, 7.0),
    ("2017-12-30", "Comedy", 1, 3.0),
    ("2017-12-31", "Action", 2, 10.0),
    ("2017-12-31", "Drama", 1, 4.0),
    ("2017-12-31", "UNKNOWN", 1, 2.5),
    ("2018-01-05", "Action", 2, 8.0),
    ("2018-01-05", "Comedy", 2, 9.0),
    ("2018-01-05", "UNKNOWN", 1, 3.0),
    ("2018-01-06", "Action", 1, 4.5),
    ("2018-01-06", "Drama", 2, 8.0),
]

BATCH_META = {"batch_run_id": BATCH_RUN_ID, "batch_cutoff_time": BATCH_CUTOFF_TIME}


def period_start(period_type: str, day: str) -> str:
    """Đầu kỳ UTC (contract 6.1); analytics_genre_trends hỗ trợ day/month/year."""
    return {"day": day, "month": day[:7] + "-01", "year": day[:4] + "-01-01"}[period_type]


def stats(count: int, total: float) -> dict:
    return {"interaction_count": count, "rating_count": count, "rating_sum": total, "avg_rating": total / count}


def build_sources() -> list[dict]:
    return [
        {
            **BATCH_META,
            "generated_at": GENERATED_AT,
            "date": day,
            "genre": genre,
            **stats(count, total),
        }
        for day, genre, count, total in sorted(DAILY)
    ]


def aggregate(period_type: str) -> dict[tuple[str, str], list[float]]:
    """(period_start, genre) -> [count, sum] cho một loại kỳ, cộng từ dữ liệu ngày."""
    acc: dict[tuple[str, str], list[float]] = defaultdict(lambda: [0, 0.0])
    for day, genre, count, total in DAILY:
        cell = acc[(period_start(period_type, day), genre)]
        cell[0] += count
        cell[1] += total
    return acc


def analytics_meta(manifest_hash: str) -> dict:
    return {
        "schema_version": SCHEMA_VERSION,
        "analytics_run_id": ANALYTICS_RUN_ID,
        "published_at": PUBLISHED_AT,
        **BATCH_META,
        "ref_version": REF_VERSION,
        "manifest_hash": manifest_hash,
    }


def build_analytics(manifest_hash: str) -> list[dict]:
    meta, rows = analytics_meta(manifest_hash), []
    for period_type in ("day", "month", "year"):
        by_period: dict[str, list[tuple[str, int, float]]] = defaultdict(list)
        for (start, genre), (count, total) in aggregate(period_type).items():
            by_period[start].append((genre, count, total))
        for start in sorted(by_period):
            # count giảm dần, hòa thì genre tăng dần theo thứ tự từ điển
            ranked = sorted(by_period[start], key=lambda item: (-item[1], item[0]))
            for rank, (genre, count, total) in enumerate(ranked, start=1):
                rows.append(
                    {
                        **meta,
                        "period_type": period_type,
                        "period_start": start,
                        "genre": genre,
                        **stats(count, total),
                        "rank_in_period": rank,
                    }
                )
    return rows


def build_manifest() -> dict:
    """Manifest giả lập đúng dạng /batch/_meta/run_id=<id>/manifest.json của Module 04."""
    return {
        "run_id": BATCH_RUN_ID,
        "batch_cutoff_time": BATCH_CUTOFF_TIME,
        "generated_at": GENERATED_AT,
        "contract_version": "1.0-draft",
        "semantics": "event_time < batch_cutoff_time",
        "reference": {"ref_version": REF_VERSION},
        "output_format": {"files": "part-NNNNN", "delimiter": "tab", "header": False},
        "multi_label": True,
        "fixture": True,
        "note": "FIXTURE P3 dựng tay, không phải kết quả MapReduce thật",
    }


def jsonl(rows: list[dict]) -> str:
    return "".join(json.dumps(r, ensure_ascii=False, sort_keys=True) + "\n" for r in rows)


def render() -> dict[str, str]:
    manifest_text = json.dumps(build_manifest(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    manifest_hash = hashlib.sha256(manifest_text.encode("utf-8")).hexdigest()
    sources = build_sources()
    return {
        "manifest.json": manifest_text,
        "genre_trends.jsonl": jsonl(sources),
        "analytics_genre_trends.jsonl": jsonl(build_analytics(manifest_hash)),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check", action="store_true", help="không ghi, chỉ báo lỗi nếu fixtures/ lệch với script")
    args = parser.parse_args()
    files = render()
    if args.check:
        stale = [
            name
            for name, text in files.items()
            if not (FIXTURES_DIR / name).is_file() or (FIXTURES_DIR / name).read_text(encoding="utf-8") != text
        ]
        if stale:
            print(f"[ERROR] Fixture lệch với build_fixtures.py: {', '.join(stale)}", file=sys.stderr)
            return 1
        print(f"[OK] {len(files)} file fixture khớp build_fixtures.py")
        return 0
    FIXTURES_DIR.mkdir(parents=True, exist_ok=True)
    for name, text in files.items():
        with open(FIXTURES_DIR / name, "w", encoding="utf-8", newline="\n") as f:
            f.write(text)
        print(f"[OK] {name}: {text.count(chr(10))} dòng")
    return 0


if __name__ == "__main__":
    sys.exit(main())
