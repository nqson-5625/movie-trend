"""Loader dữ liệu thật của lane movie_batch (P2.2): đọc export Module 04 và chặn dữ liệu sai trước khi ghi ES.

Chạy offline, không cần Elasticsearch:
    .venv/bin/python -m pytest modules/elasticsearch/tests/lanes -q
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from modules.elasticsearch.lanes.movie_batch.load_batch_run import DEFAULT_RUN_DIR, physical_index, read_run

RUN_ID, T, GEN = "20261024T104145Z", "2018-10-01T00:00:00Z", "2026-10-24T10:41:45Z"
TAIL = [RUN_ID, T, GEN]
MOVIE_COLUMNS = ["movieId", "title", "total_count", "rating_count", "rating_sum", "avg_rating",
                 "batch_run_id", "batch_cutoff_time", "generated_at"]
DAILY_COLUMNS = ["date", "movieId", "interaction_count", "rating_count", "rating_sum", "avg_rating",
                 "batch_run_id", "batch_cutoff_time", "generated_at"]
MOVIES = [["1", "Toy Story (1995)", "3", "3", "12.0", "4.0"], ["99", "", "1", "1", "2.5", "2.5"]]
DAILY = [["2018-09-29", "1", "2", "2", "8.0", "4.0"], ["2018-09-30", "1", "1", "1", "4.0", "4.0"],
         ["2018-09-30", "99", "1", "1", "2.5", "2.5"]]


def write_run(tmp_path: Path, movies=MOVIES, daily=DAILY, header=True, counters=None) -> Path:
    """Thư mục run giống `make -C modules/mapreduce download` tạo ra."""
    counters = counters or {"movie_stats": len(movies), "movie_daily_stats": len(daily)}
    manifest = {
        "run_id": RUN_ID, "batch_cutoff_time": T, "generated_at": GEN,
        "reference": {"ref_version": "20261017-5f26995f"},
        "jobs": {job: {"columns": cols, "counters": {"framework": {"Reduce output records": counters[job]}}}
                 for job, cols in (("movie_stats", MOVIE_COLUMNS), ("movie_daily_stats", DAILY_COLUMNS))},
    }
    (tmp_path / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    for job, cols, rows in (("movie_stats", MOVIE_COLUMNS, movies), ("movie_daily_stats", DAILY_COLUMNS, daily)):
        lines = (["\t".join(cols)] if header else []) + ["\t".join(r + TAIL) for r in rows]
        (tmp_path / f"{job}.tsv").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return tmp_path


@pytest.mark.parametrize("header", [True, False])
def test_reads_export_with_or_without_header(tmp_path, header):
    run = read_run(write_run(tmp_path, header=header))
    movies, daily = run.docs["movie_batch_stats"], run.docs["movie_daily_stats"]
    assert movies[0] == {"movieId": 1, "title": "Toy Story (1995)", "total_count": 3, "rating_count": 3,
                         "rating_sum": 12.0, "avg_rating": 4.0, "batch_run_id": RUN_ID,
                         "batch_cutoff_time": T, "generated_at": GEN}
    assert movies[1]["title"] is None, "title rỗng (title_missing) phải thành null"
    assert daily[0]["date"] == "2018-09-29" and isinstance(daily[0]["interaction_count"], int)
    assert (run.ref_version, len(run.manifest_sha256)) == ("20261017-5f26995f", 64)


def test_physical_index_uses_lowercase_run_id():
    assert physical_index("movie_daily_stats", RUN_ID) == "movie_daily_stats_20261024t104145z"
    assert physical_index("movie_batch_stats", RUN_ID, "dev_p2_") == "dev_p2_movie_batch_stats_20261024t104145z"


@pytest.mark.parametrize("case, kwargs, message", [
    ("thiếu cột", {"daily": [r[:-1] for r in DAILY]}, "cột"),
    ("lệch số dòng với manifest", {"counters": {"movie_stats": 2, "movie_daily_stats": 4}}, "Reduce output records"),
    ("tổng theo ngày khác bảng phim", {"daily": DAILY[:1] + DAILY[2:]}, "cộng theo ngày"),
    ("avg không bằng sum/count", {"movies": [MOVIES[0][:5] + ["3.9"], MOVIES[1]]}, "không nhất quán"),
    ("ngày không trước mốc T", {"daily": DAILY[:2] + [["2018-10-01", "99", "1", "1", "2.5", "2.5"]]}, "mốc T"),
    ("trùng khóa ngày-phim", {"daily": DAILY + [DAILY[0]], "counters": {"movie_stats": 2, "movie_daily_stats": 4}},
     "trùng"),
    ("rating ngoài [0.5, 5]", {"movies": [MOVIES[0][:4] + ["18.0", "6.0"], MOVIES[1]]}, "dòng 1"),
])
def test_rejects_bad_export_before_writing(tmp_path, case, kwargs, message):
    with pytest.raises(ValueError, match=message):
        read_run(write_run(tmp_path, **kwargs))


def test_rejects_rows_from_another_run(tmp_path):
    path = write_run(tmp_path)
    tsv = path / "movie_stats.tsv"
    tsv.write_text(tsv.read_text(encoding="utf-8").replace(RUN_ID, "20261028T150122Z"), encoding="utf-8")
    with pytest.raises(ValueError, match="khác manifest"):
        read_run(path)


@pytest.mark.skipif(not (DEFAULT_RUN_DIR / "manifest.json").is_file(), reason="Chưa tải run 20261024T104145Z về máy")
def test_official_run_export_is_complete():
    run = read_run(DEFAULT_RUN_DIR)
    movies, daily = run.docs["movie_batch_stats"], run.docs["movie_daily_stats"]
    assert (len(movies), len(daily)) == (9724, 99538)
    assert sum(d["total_count"] for d in movies) == sum(d["interaction_count"] for d in daily) == 100836
    assert run.ref_version == "20261017-5f26995f"
