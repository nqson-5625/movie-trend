"""Sinh fixture lane movielens_realtime (P4) cho Phase2-3.0 từ một danh sách rating viết tay.

Nguồn sự thật duy nhất là EVENTS + REFERENCE bên dưới; mọi bảng còn lại được tính từ đó nên luôn khớp nhau:
  movie_realtime_stats, genre_realtime (source, đúng dạng output Spark Streaming Module 05),
  analytics_growth (lịch sử) và analytics_growth_current (2 snapshot READY + 1 snapshot phải bị REJECTED).
Fixture phủ các ca contract mục 9 yêu cầu cho P4: hai size 10m/1h; mỗi size có window cũ, window được chọn
(complete) và window mới hơn chưa complete; movie + genre; previous có/thiếu (kể cả thiếu nhưng có window
cũ hơn không liền kề); score = 0 khác null; hai physical current versions để thử rollback.

    .venv/bin/python contracts/phase2/movielens_realtime/build_fixtures.py          # ghi lại fixtures/
    .venv/bin/python contracts/phase2/movielens_realtime/build_fixtures.py --check  # chỉ kiểm tra fixtures/ còn đúng
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

LANE_DIR = Path(__file__).resolve().parent
FIXTURES_DIR = LANE_DIR / "fixtures"

SCHEMA_VERSION = "phase2-3.0"
TIMELINE_MODE = "replay_history"
BATCH_CUTOFF_TIME = "2018-10-01T00:00:00Z"  # T của Batch; replay MovieLens hiện có đều trước T
SOURCE_UPDATED_AT = "2026-11-26T00:00:00Z"  # processing time giả lập của Spark Streaming
HISTORY_RUN_ID = "fixture-p4-history-g2"
HISTORY_PUBLISHED_AT = "2026-11-26T02:00:00Z"
WINDOW_SIZES = {"10m": timedelta(minutes=10), "1h": timedelta(hours=1)}

# Trích từ contracts/samples/movie-reference.sample.tsv (ref_version 20261010-01e56f1d).
# movieId 999001 cố ý KHÔNG có trong reference: title=null và genre=UNKNOWN (join miss).
REF_VERSION = "20261010-01e56f1d"
REFERENCE = {
    218: ("Boys on the Side (1995)", "Comedy|Drama"),
    222: ("Circle of Friends (1995)", "Drama|Romance"),
    227: ("Drop Zone (1994)", "Action|Thriller"),
    343: ("Baby-Sitters Club, The (1995)", "Children"),
}

# (event_time UTC, movieId, rating) — mỗi dòng là một rating MovieLens đã dedup.
EVENTS = [
    # 1h 09:00 — window cũ
    ("2018-09-20T09:15:00Z", 222, 4.0),
    ("2018-09-20T09:20:00Z", 343, 3.0),
    ("2018-09-20T09:30:00Z", 227, 3.5),
    # 1h 10:00 — previous của 1h được chọn ở snapshot s002, đồng thời là 1h được chọn ở s001
    ("2018-09-20T10:05:00Z", 222, 5.0),
    ("2018-09-20T10:10:00Z", 222, 4.0),
    ("2018-09-20T10:30:00Z", 218, 2.0),
    ("2018-09-20T10:45:00Z", 343, 3.5),
    # 10m 11:30 — window cũ
    ("2018-09-20T11:31:00Z", 343, 4.0),
    ("2018-09-20T11:33:00Z", 999001, 2.0),
    ("2018-09-20T11:35:00Z", 218, 3.0),
    # 10m 11:40 — previous của 10m được chọn ở s002, đồng thời là 10m được chọn ở s001
    ("2018-09-20T11:41:00Z", 222, 4.5),
    ("2018-09-20T11:42:00Z", 218, 3.0),
    ("2018-09-20T11:44:00Z", 218, 4.0),
    ("2018-09-20T11:46:00Z", 227, 2.5),
    # 10m 11:50 — được chọn ở s002
    ("2018-09-20T11:51:00Z", 222, 5.0),
    ("2018-09-20T11:52:00Z", 222, 4.0),
    ("2018-09-20T11:53:00Z", 222, 3.5),
    ("2018-09-20T11:54:00Z", 218, 2.0),
    ("2018-09-20T11:55:00Z", 227, 3.0),
    ("2018-09-20T11:57:00Z", 999001, 1.5),
    # 10m 12:00 và 1h 12:00 — kết thúc sau source_complete_through, KHÔNG được vào current
    ("2018-09-20T12:01:00Z", 222, 4.0),
    ("2018-09-20T12:02:00Z", 343, 5.0),
    ("2018-09-20T12:03:00Z", 343, 4.5),
]

# Mỗi snapshot: mốc complete đã công bố (fixture giả lập replay EOF/watermark) và kết quả mong đợi.
SNAPSHOTS = [
    {"snapshot_version": "fixture-p4-s001", "source_complete_through": "2018-09-20T11:50:00Z",
     "history_analytics_run_id": "fixture-p4-history-g1", "published_at": "2026-11-26T01:00:00Z",
     "expected_status": "READY", "reject_reason": None, "sizes": ("10m", "1h")},
    {"snapshot_version": "fixture-p4-s002", "source_complete_through": "2018-09-20T12:00:00Z",
     "history_analytics_run_id": "fixture-p4-history-g2", "published_at": "2026-11-26T02:00:00Z",
     "expected_status": "READY", "reject_reason": None, "sizes": ("10m", "1h")},
    # Cố ý thiếu size 1h: publisher/READY check phải từ chối, alias giữ s002 (contract 6.2 bước 3-4, mục 12).
    {"snapshot_version": "fixture-p4-s003", "source_complete_through": "2018-09-20T12:00:00Z",
     "history_analytics_run_id": "fixture-p4-history-g2", "published_at": "2026-11-26T03:00:00Z",
     "expected_status": "REJECTED", "reject_reason": "missing_window_size_1h", "sizes": ("10m",)},
]

TS_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def parse_ts(text: str) -> datetime:
    return datetime.strptime(text, TS_FORMAT).replace(tzinfo=timezone.utc)


def fmt_ts(value: datetime) -> str:
    return value.strftime(TS_FORMAT)


def window_start(ts: datetime, size: timedelta) -> datetime:
    """Cửa sổ tumbling nửa mở [start, end) align UTC, giống F.window của Spark."""
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    return ts - ((ts - epoch) % size)


def genres_of(movie_id: int) -> list[str]:
    """Giống compute_genre_realtime: join miss hoặc '(no genres listed)' -> UNKNOWN."""
    genres = REFERENCE.get(movie_id, (None, None))[1]
    if not genres or genres == "(no genres listed)":
        return ["UNKNOWN"]
    return genres.split("|")


def aggregate(entity_of) -> list[dict]:
    """Gom EVENTS theo (size, window_start, entity) cho cả hai size; entity_of(movieId) trả list entity."""
    acc: dict[tuple, list] = defaultdict(lambda: [0, 0.0])
    for event_time, movie_id, rating in EVENTS:
        for size_label, size in WINDOW_SIZES.items():
            start = window_start(parse_ts(event_time), size)
            for entity in entity_of(movie_id):
                cell = acc[(size_label, start, entity)]
                cell[0] += 1
                cell[1] += rating
    return [
        {"window_start": fmt_ts(start), "window_end": fmt_ts(start + WINDOW_SIZES[size_label]),
         "window_size": size_label, "entity": entity, "interaction_count": count, "rating_count": count,
         "rating_sum": round(total, 4), "avg_rating": round(total / count, 4), "updated_at": SOURCE_UPDATED_AT}
        for (size_label, start, entity), (count, total) in sorted(acc.items(), key=lambda kv: (kv[0][0], kv[0][1], str(kv[0][2])))
    ]


def build_sources() -> tuple[list[dict], list[dict]]:
    movies = [{**{k: v for k, v in r.items() if k != "entity"}, "movieId": r["entity"]} for r in aggregate(lambda m: [m])]
    genres = [{**{k: v for k, v in r.items() if k != "entity"}, "genre": r["entity"]} for r in aggregate(genres_of)]
    return movies, genres


def trending_score(current: int, previous: int | None) -> float | None:
    """Contract mục 5: (current - previous) / (previous + 1); previous không tồn tại -> null (không phải 0)."""
    if previous is None:
        return None
    return (current - previous) / (previous + 1)


def build_history(movies: list[dict], genres: list[dict]) -> list[dict]:
    """analytics_growth: một dòng cho MỖI source window; previous là đúng window_start - window_size."""
    rows = []
    for entity_type, source, key_field in (("movie", movies, "movieId"), ("genre", genres, "genre")):
        index = {(r["window_size"], r["window_start"], r[key_field]): r for r in source}
        for r in source:
            prev_start = fmt_ts(parse_ts(r["window_start"]) - WINDOW_SIZES[r["window_size"]])
            prev = index.get((r["window_size"], prev_start, r[key_field]))
            previous = prev["interaction_count"] if prev else None
            if entity_type == "movie":
                title = REFERENCE.get(r["movieId"], (None, None))[0]
                movie_id, genre = r["movieId"], None
            else:
                title, movie_id, genre = None, None, r["genre"]
            rows.append({
                "schema_version": SCHEMA_VERSION, "analytics_run_id": HISTORY_RUN_ID,
                "published_at": HISTORY_PUBLISHED_AT,
                "window_start": r["window_start"], "window_end": r["window_end"], "window_size": r["window_size"],
                "entity_type": entity_type, "entity_key": str(r[key_field]), "movieId": movie_id, "genre": genre,
                "title": title, "title_source": f"movie_reference/{REF_VERSION}" if title else None,
                "current_window_count": r["interaction_count"], "previous_window_count": previous,
                "previous_window_present": prev is not None,
                "trending_score": trending_score(r["interaction_count"], previous),
                "rating_count": r["rating_count"], "rating_sum": r["rating_sum"], "avg_rating": r["avg_rating"],
                "updated_at": r["updated_at"], "timeline_mode": TIMELINE_MODE,
            })
    return sorted(rows, key=lambda r: (r["window_size"], r["window_start"], r["entity_type"], r["entity_key"]))


def selected_window_start(history: list[dict], size: str, complete_through: str) -> str:
    """Contract 6.2 bước 1: W_s = max(window_start) với window_end <= source_complete_through."""
    return max(r["window_start"] for r in history
               if r["window_size"] == size and r["window_end"] <= complete_through)


def build_current(history: list[dict], snap: dict) -> list[dict]:
    rows = []
    for size in snap["sizes"]:
        w = selected_window_start(history, size, snap["source_complete_through"])
        for h in history:
            if h["window_size"] == size and h["window_start"] == w:
                rows.append({
                    **h,
                    "analytics_run_id": f"fixture-p4-current-{snap['snapshot_version'].rsplit('-', 1)[-1]}",
                    "published_at": snap["published_at"],
                    "snapshot_version": snap["snapshot_version"], "selected_window_start": w,
                    "source_complete_through": snap["source_complete_through"],
                    "history_analytics_run_id": snap["history_analytics_run_id"],
                })
    return rows


def build_snapshots_manifest() -> dict:
    return {
        "fixture": True,
        "note": "FIXTURE P4 dựng tay, không phải replay MovieLens thật",
        "schema_version": SCHEMA_VERSION,
        "complete_basis": "fixture_declared_watermark",
        "covers": ["movie_realtime_stats", "genre_realtime"],
        "batch_cutoff_time": BATCH_CUTOFF_TIME,
        "snapshots": [
            {**{k: v for k, v in s.items() if k != "sizes"}, "window_sizes": list(s["sizes"]),
             "fixture_file": f"analytics_growth_current.{s['snapshot_version']}.jsonl",
             "physical_index": f"analytics_growth_current-v{s['snapshot_version']}"}
            for s in SNAPSHOTS
        ],
    }


def jsonl(rows: list[dict]) -> str:
    return "".join(json.dumps(r, ensure_ascii=False, sort_keys=True) + "\n" for r in rows)


def render() -> dict[str, str]:
    movies, genres = build_sources()
    history = build_history(movies, genres)
    files = {
        "snapshots.json": json.dumps(build_snapshots_manifest(), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        "movie_realtime_stats.jsonl": jsonl(movies),
        "genre_realtime.jsonl": jsonl(genres),
        "analytics_growth.jsonl": jsonl(history),
    }
    for snap in SNAPSHOTS:
        files[f"analytics_growth_current.{snap['snapshot_version']}.jsonl"] = jsonl(build_current(history, snap))
    return files


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check", action="store_true", help="không ghi, chỉ báo lỗi nếu fixtures/ lệch với script")
    args = parser.parse_args()
    files = render()
    if args.check:
        stale = [name for name, text in files.items()
                 if not (FIXTURES_DIR / name).is_file() or (FIXTURES_DIR / name).read_text(encoding="utf-8") != text]
        extra = sorted(p.name for p in FIXTURES_DIR.glob("*") if p.name not in files)
        if stale or extra:
            print(f"[ERROR] Fixture lệch với build_fixtures.py: {', '.join(stale + extra)}", file=sys.stderr)
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
