"""Kiểm tra 3 job MapReduce chạy bằng pipe `mapper | sort | reducer` (giống Hadoop Streaming) trên máy dev.

- Trường hợp đặc biệt dựng tay: event lỗi, event sau mốc T, event trùng, phim không có/không tìm thấy thể loại.
- Toàn bộ dữ liệu MovieLens thật, mỗi event gửi 2 lần, đối chiếu với pandas tính độc lập.
"""
import csv
import json
import shutil
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest

from modules.data_source.src.movie_event_adapter import to_movie_event

ROOT = Path(__file__).resolve().parents[3]
SRC = ROOT / "modules/mapreduce/src"
RATINGS = ROOT / "data/prepared/movielens/ratings_sorted.csv"
REFERENCE = ROOT / "data/prepared/reference/movie_reference.tsv"
INGEST = "2026-10-24T00:00:00Z"
TAIL = {"BATCH_RUN_ID": "test", "GENERATED_AT": "2026-10-24T00:00:00Z"}
COLUMNS = {
    "movie_stats": ["movieId", "title", "total_count", "rating_count", "rating_sum", "avg_rating"],
    "movie_daily_stats": ["date", "movieId", "interaction_count", "rating_count", "rating_sum", "avg_rating"],
    "genre_daily_stats": ["date", "genre", "interaction_count", "rating_count", "rating_sum", "avg_rating"],
}
TAIL_COLUMNS = ["batch_run_id", "batch_cutoff_time", "generated_at"]


def event(user, movie, rating, ts):
    return json.dumps(to_movie_event({"userId": user, "movieId": movie, "rating": rating, "timestamp": ts}, INGEST))


def stage_like_hadoop(workdir):
    """Giống -files của Hadoop: mỗi file nằm trong một thư mục cache riêng, thư mục làm việc chỉ có symlink.
    Python dò module theo thư mục THẬT của script, nên phải chạy với PYTHONPATH=. như run_batch.sh."""
    for f in SRC.glob("*.py"):
        cache = workdir / "filecache" / f.stem
        cache.mkdir(parents=True, exist_ok=True)
        shutil.copy(f, cache / f.name)
        link = workdir / f.name
        if not link.exists():
            link.symlink_to(cache / f.name)


def run_job(job, lines, workdir, cutoff):
    """mapper | sort theo key (byte) | reducer, chạy trong workdir có movie_reference.tsv như trên cụm."""
    stage_like_hadoop(workdir)
    env = {"PATH": "/usr/bin:/bin", "PYTHONPATH": ".", "BATCH_CUTOFF_TIME": cutoff, **TAIL}
    mapped = subprocess.run([sys.executable, f"{job}_mapper.py"], input="\n".join(lines) + "\n", text=True,
                            capture_output=True, cwd=workdir, env=env, check=True)
    shuffled = "".join(sorted(mapped.stdout.splitlines(keepends=True), key=lambda l: l.split("\t", 1)[0].encode()))
    reduced = subprocess.run([sys.executable, f"{job}_reducer.py"], input=shuffled, text=True,
                             capture_output=True, cwd=workdir, env=env, check=True)
    counters = {}
    for line in (mapped.stderr + reduced.stderr).splitlines():
        if line.startswith("reporter:counter:"):
            _, name, amount = line.split(":", 2)[2].split(",")
            counters[name] = counters.get(name, 0) + int(amount)
    rows = [dict(zip(COLUMNS[job] + TAIL_COLUMNS, r.split("\t"))) for r in reduced.stdout.splitlines()]
    return rows, counters


def write_reference(path, movies):
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(["movieId", "title", "release_year", "genres", "imdbId", "tmdbId", "imdb_rating",
                    "imdb_votes", "ref_version", "generated_at"])
        for movie_id, (title, genres) in movies.items():
            w.writerow([movie_id, title, "", genres, "", "", "", "", "test", INGEST])


# ---------- trường hợp dựng tay ----------
T = "2018-01-01T00:00:00Z"
T_EPOCH = 1514764800
DAY1 = T_EPOCH - 2 * 86400 + 3600          # 2017-12-30T01:00:00Z


@pytest.fixture()
def small(tmp_path):
    write_reference(tmp_path / "movie_reference.tsv",
                    {1: ("Movie A", "Action|Comedy"), 2: ("Movie B", "(no genres listed)"), 3: ("Movie C", "")})
    e1 = event(1, 1, 4.0, DAY1)
    lines = [
        e1, e1,                                  # gửi trùng -> tính 1 lần
        event(2, 1, 2.0, DAY1),
        event(3, 2, 5.0, DAY1),                  # "(no genres listed)" -> UNKNOWN
        event(4, 3, 3.0, DAY1),                  # genres rỗng -> UNKNOWN
        event(5, 99, 1.0, DAY1),                 # không có trong reference -> UNKNOWN, join miss
        event(6, 1, 5.0, T_EPOCH - 1),           # giây cuối trước T -> giữ (ngày 2017-12-31)
        event(7, 1, 5.0, T_EPOCH),               # đúng T -> thuộc Realtime, bỏ
        "not json",
        json.dumps({**json.loads(event(8, 1, 4.0, DAY1)), "rating": 9.0}),
    ]
    return tmp_path, lines


def test_movie_stats_small(small):
    workdir, lines = small
    rows, c = run_job("movie_stats", lines, workdir, T)
    by = {r["movieId"]: r for r in rows}
    assert (by["1"]["total_count"], float(by["1"]["rating_sum"])) == ("3", 11.0)   # 4 + 2 + 5
    assert by["1"]["title"] == "Movie A" and by["99"]["title"] == ""
    assert set(by) == {"1", "2", "3", "99"}
    assert c["duplicates_removed"] == 1 and c["invalid_records"] == 2 and c["after_cutoff_skipped"] == 1
    assert c["title_missing"] == 1
    assert all(r["batch_cutoff_time"] == T and r["batch_run_id"] == "test" for r in rows)


def test_movie_daily_stats_small(small):
    workdir, lines = small
    rows, _ = run_job("movie_daily_stats", lines, workdir, T)
    by = {(r["date"], r["movieId"]): r for r in rows}
    assert by[("2017-12-30", "1")]["interaction_count"] == "2"
    assert by[("2017-12-31", "1")]["interaction_count"] == "1"
    assert ("2018-01-01", "1") not in by


def test_genre_daily_stats_small(small):
    workdir, lines = small
    rows, c = run_job("genre_daily_stats", lines, workdir, T)
    by = {(r["date"], r["genre"]): r for r in rows}
    for genre in ("Action", "Comedy"):             # phim 1 có 2 thể loại -> explode
        assert (by[("2017-12-30", genre)]["interaction_count"], float(by[("2017-12-30", genre)]["rating_sum"])) == ("2", 6.0)
        assert by[("2017-12-31", genre)]["interaction_count"] == "1"
    unknown = by[("2017-12-30", "UNKNOWN")]        # phim 2 (no genres listed) + phim 3 (rỗng) + phim 99 (không có)
    assert (unknown["interaction_count"], float(unknown["rating_sum"])) == ("3", 9.0)
    assert c["genre_join_miss"] == 1 and c["unknown_genre_events"] == 2
    assert not any(r["genre"] == "(no genres listed)" for r in rows)


def test_genre_job_requires_reference(tmp_path):
    stage_like_hadoop(tmp_path)
    proc = subprocess.run([sys.executable, "genre_daily_stats_mapper.py"], input="", text=True, capture_output=True,
                          cwd=tmp_path, env={"PATH": "/usr/bin:/bin", "PYTHONPATH": ".", "BATCH_CUTOFF_TIME": T})
    assert proc.returncode != 0 and "movie_reference.tsv" in proc.stderr


# ---------- toàn bộ dữ liệu thật, đối chiếu pandas ----------
@pytest.fixture(scope="module")
def full():
    if not (RATINGS.is_file() and REFERENCE.is_file()):
        pytest.skip("Chưa có dữ liệu prepared của Module 01")
    with RATINGS.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    lines = [json.dumps(to_movie_event(r, INGEST)) for r in rows]
    df = pd.DataFrame(rows).astype({"movieId": int, "rating": float, "timestamp": int})
    df = df[df.timestamp < T_EPOCH]
    df["date"] = pd.to_datetime(df.timestamp, unit="s", utc=True).dt.strftime("%Y-%m-%d")
    return lines + lines, df                      # mỗi event gửi 2 lần


def check(rows, expected, keys):
    got = pd.DataFrame(rows)
    for k in keys:
        if k == "movieId":
            got[k] = got[k].astype(int)
    got = got.set_index(keys).sort_index()
    expected = expected.sort_index()
    assert list(got.index) == list(expected.index)
    assert (got.iloc[:, 0].astype(int).to_numpy() == expected["size"].to_numpy()).all()
    assert (got["rating_sum"].astype(float) - expected["sum"]).abs().max() < 1e-9
    assert ((got["avg_rating"].astype(float) - got["rating_sum"].astype(float) / got["rating_count"].astype(int)).abs() < 1e-9).all()


def test_full_data_matches_pandas(full, tmp_path):
    lines, df = full
    shutil.copy(REFERENCE, tmp_path / "movie_reference.tsv")
    kept = len(df)

    rows, c = run_job("movie_stats", lines, tmp_path, T)
    check([{k: r[k] for k in COLUMNS["movie_stats"] if k != "title"} for r in rows],
          df.groupby("movieId").rating.agg(["size", "sum"]), ["movieId"])
    assert c["duplicates_removed"] == kept and "invalid_records" not in c

    rows, _ = run_job("movie_daily_stats", lines, tmp_path, T)
    check(rows, df.groupby(["date", "movieId"]).rating.agg(["size", "sum"]), ["date", "movieId"])

    ref = pd.read_csv(REFERENCE, sep="\t", dtype=str, keep_default_na=False)
    ref["movieId"] = ref.movieId.astype(int)
    ref["genre"] = ref.genres.apply(lambda g: [x for x in g.split("|") if x and x != "(no genres listed)"] or ["UNKNOWN"])
    exploded = df.merge(ref[["movieId", "genre"]], on="movieId", how="left").explode("genre").fillna({"genre": "UNKNOWN"})
    rows, _ = run_job("genre_daily_stats", lines, tmp_path, T)
    check(rows, exploded.groupby(["date", "genre"]).rating.agg(["size", "sum"]), ["date", "genre"])
