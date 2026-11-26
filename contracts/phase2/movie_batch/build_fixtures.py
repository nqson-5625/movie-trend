"""Sinh fixture lane movie_batch (P2) cho Phase2-3.0 từ một bảng ngày viết tay.

Nguồn sự thật duy nhất là DAILY bên dưới; bốn bảng còn lại được tính từ đó nên luôn khớp nhau:
  movie_batch_stats, movie_daily_stats (source) và analytics_timeseries, analytics_top_movies (analytics).
Fixture phủ các ca contract mục 9 yêu cầu cho P2: dòng all có period_start=null; day/month/year;
entity movie/all; Top hai kỳ và ca hòa; kỳ trống (2018-02 không có rating, trước mốc T).

    .venv/bin/python contracts/phase2/movie_batch/build_fixtures.py          # ghi lại fixtures/
    .venv/bin/python contracts/phase2/movie_batch/build_fixtures.py --check  # chỉ kiểm tra fixtures/ còn đúng
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
BATCH_RUN_ID = "fixture-p2-v1"
BATCH_CUTOFF_TIME = "2018-03-01T00:00:00Z"
GENERATED_AT = "2026-11-24T00:00:00Z"
REF_VERSION = "fixture-ref-v1"
ANALYTICS_RUN_ID = "fixture-p2-v1-analytics"
PUBLISHED_AT = "2026-11-24T00:00:00Z"

# movieId -> title; 99 cố ý không có title để thử nhánh title=null
TITLES = {1: "Toy Story (1995)", 2: "Jumanji (1995)", 3: "Grumpier Old Men (1995)", 99: None}

# (date, movieId, rating_count, rating_sum) — mỗi phim/ngày một dòng, giống movie_daily_stats.tsv
DAILY = [
    ("2017-12-30", 1, 2, 7.0),
    ("2017-12-30", 2, 1, 3.0),
    ("2017-12-31", 1, 1, 4.0),
    ("2017-12-31", 3, 2, 6.0),
    ("2018-01-05", 1, 1, 5.0),
    ("2018-01-05", 2, 2, 9.0),
    ("2018-01-05", 99, 1, 2.5),
    ("2018-01-06", 2, 1, 4.0),
    ("2018-01-06", 3, 1, 3.5),
]

BATCH_META = {"batch_run_id": BATCH_RUN_ID, "batch_cutoff_time": BATCH_CUTOFF_TIME}


def period_start(period_type: str, day: str) -> str | None:
    """Đầu kỳ UTC (contract 6.1); kỳ all không có period_start."""
    return {"day": day, "month": day[:7] + "-01", "year": day[:4] + "-01-01", "all": None}[period_type]


def stats(count: int, total: float) -> dict:
    return {"interaction_count": count, "rating_count": count, "rating_sum": total, "avg_rating": total / count}


def build_sources() -> tuple[list[dict], list[dict]]:
    daily = [
        {**BATCH_META, "generated_at": GENERATED_AT, "date": day, "movieId": movie,
         "interaction_count": count, "rating_count": count, "rating_sum": total, "avg_rating": total / count}
        for day, movie, count, total in sorted(DAILY)
    ]
    per_movie: dict[int, list[float]] = defaultdict(lambda: [0, 0.0])
    for _, movie, count, total in DAILY:
        per_movie[movie][0] += count
        per_movie[movie][1] += total
    movies = []
    for movie in sorted(per_movie):
        count, total = per_movie[movie]
        doc = {**BATCH_META, "generated_at": GENERATED_AT, "movieId": movie, "title": TITLES[movie],
               "total_count": count, "rating_count": count, "rating_sum": total, "avg_rating": total / count}
        movies.append(doc)
    return movies, daily


def aggregate(period_type: str) -> dict[tuple[str | None, int], list[float]]:
    """(period_start, movieId) -> [count, sum] cho một loại kỳ, cộng từ dữ liệu ngày."""
    acc: dict[tuple[str | None, int], list[float]] = defaultdict(lambda: [0, 0.0])
    for day, movie, count, total in DAILY:
        cell = acc[(period_start(period_type, day), movie)]
        cell[0] += count
        cell[1] += total
    return acc


def analytics_meta(manifest_hash: str) -> dict:
    return {"schema_version": SCHEMA_VERSION, "analytics_run_id": ANALYTICS_RUN_ID, "published_at": PUBLISHED_AT,
            **BATCH_META, "ref_version": REF_VERSION, "manifest_hash": manifest_hash}


def build_timeseries(manifest_hash: str) -> list[dict]:
    meta, rows = analytics_meta(manifest_hash), []
    for period_type in ("day", "month", "year", "all"):
        acc = aggregate(period_type)
        totals: dict[str | None, list] = defaultdict(lambda: [0, 0.0, set()])
        for (start, movie), (count, total) in sorted(acc.items(), key=lambda kv: (kv[0][0] or "", kv[0][1])):
            rows.append({**meta, "period_type": period_type, "period_start": start, "entity_type": "movie",
                         "movieId": movie, "title": TITLES[movie], **stats(count, total),
                         "distinct_movie_count": None})
            totals[start][0] += count
            totals[start][1] += total
            totals[start][2].add(movie)
        for start in sorted(totals, key=lambda s: s or ""):
            count, total, movies = totals[start]
            rows.append({**meta, "period_type": period_type, "period_start": start, "entity_type": "all",
                         "movieId": None, "title": None, **stats(count, total),
                         "distinct_movie_count": len(movies)})
    return rows


def build_top_movies(manifest_hash: str) -> list[dict]:
    meta, rows = analytics_meta(manifest_hash), []
    for period_type in ("month", "year", "all"):
        by_period: dict[str | None, list[tuple[int, int, float]]] = defaultdict(list)
        for (start, movie), (count, total) in aggregate(period_type).items():
            by_period[start].append((movie, count, total))
        for start in sorted(by_period, key=lambda s: s or ""):
            ranked = sorted(by_period[start], key=lambda m: (-m[1], m[0]))  # count giảm dần, hòa thì movieId tăng
            for rank, (movie, count, total) in enumerate(ranked, start=1):
                rows.append({**meta, "period_type": period_type, "period_start": start, "movieId": movie,
                             "title": TITLES[movie], **stats(count, total), "rank_in_period": rank})
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
        "fixture": True,
        "note": "FIXTURE P2 dựng tay, không phải kết quả MapReduce thật",
    }


def jsonl(rows: list[dict]) -> str:
    return "".join(json.dumps(r, ensure_ascii=False, sort_keys=True) + "\n" for r in rows)


def render() -> dict[str, str]:
    manifest_text = json.dumps(build_manifest(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    manifest_hash = hashlib.sha256(manifest_text.encode("utf-8")).hexdigest()
    movies, daily = build_sources()
    return {
        "manifest.json": manifest_text,
        "movie_batch_stats.jsonl": jsonl(movies),
        "movie_daily_stats.jsonl": jsonl(daily),
        "analytics_timeseries.jsonl": jsonl(build_timeseries(manifest_hash)),
        "analytics_top_movies.jsonl": jsonl(build_top_movies(manifest_hash)),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check", action="store_true", help="không ghi, chỉ báo lỗi nếu fixtures/ lệch với script")
    args = parser.parse_args()
    files = render()
    if args.check:
        stale = [name for name, text in files.items()
                 if not (FIXTURES_DIR / name).is_file() or (FIXTURES_DIR / name).read_text(encoding="utf-8") != text]
        if stale:
            print(f"[ERROR] Fixture lệch với build_fixtures.py: {', '.join(stale)}", file=sys.stderr)
            return 1
        print(f"[OK] {len(files)} file fixture khớp build_fixtures.py")
        return 0
    FIXTURES_DIR.mkdir(parents=True, exist_ok=True)
    for name, text in files.items():
        (FIXTURES_DIR / name).write_text(text, encoding="utf-8")
        print(f"[OK] {name}: {text.count(chr(10))} dòng")
    return 0


if __name__ == "__main__":
    sys.exit(main())
