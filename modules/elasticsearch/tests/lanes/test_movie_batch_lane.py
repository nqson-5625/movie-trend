"""Lane movie_batch (P2): index templates, JSON Schema và fixture phải khớp nhau và khớp DATA_CONTRACT_Phase2.

Chạy offline, không cần Elasticsearch:
    .venv/bin/python -m pytest modules/elasticsearch/tests/lanes -q
Fixture được kiểm bằng cách tự cộng lại từ movie_daily_stats, độc lập với build_fixtures.py.
"""
from __future__ import annotations

import fnmatch
import hashlib
import json
import subprocess
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker

from modules.spark_sql.common.writers.key_serializer import build_record_id

ROOT = Path(__file__).resolve().parents[4]
LANES_DIR = ROOT / "modules/elasticsearch/lanes"
TEMPLATES_DIR = LANES_DIR / "movie_batch/templates"
COMPONENTS_DIR = ROOT / "modules/elasticsearch/common/templates/components"
LANE_DIR = ROOT / "contracts/phase2/movie_batch"
FIXTURES_DIR = LANE_DIR / "fixtures"

SOURCES = ("movie_batch_stats", "movie_daily_stats")
ANALYTICS = ("analytics_timeseries", "analytics_top_movies")
FAMILIES = SOURCES + ANALYTICS
KEYS = {
    "movie_batch_stats": ("batch_run_id", "movieId"),
    "movie_daily_stats": ("batch_run_id", "date", "movieId"),
    "analytics_timeseries": ("batch_run_id", "period_type", "period_start", "entity_type", "movieId"),
    "analytics_top_movies": ("batch_run_id", "period_type", "period_start", "movieId"),
}
ES_TO_JSON = {"integer": "integer", "long": "integer", "double": "number", "keyword": "string", "text": "string",
              "date": "string"}


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def template(family: str) -> dict:
    return load_json(TEMPLATES_DIR / f"{family}.json")


def mapped_properties(tpl: dict) -> dict:
    """Field của template gộp với field của các component template nó compose."""
    props: dict = {}
    for name in tpl["composed_of"]:
        comp = load_json(COMPONENTS_DIR / f"{name.removeprefix('movie_trend_')}.json")
        props.update(comp["template"].get("mappings", {}).get("properties", {}))
    props.update(tpl["template"]["mappings"]["properties"])
    return props


def schema(family: str) -> dict:
    return load_json(LANE_DIR / "schemas" / f"{family}.schema.json")


def fixture(family: str) -> list[dict]:
    lines = (FIXTURES_DIR / f"{family}.jsonl").read_text(encoding="utf-8").splitlines()
    return [json.loads(line) for line in lines]


def json_types(prop: dict, defs: dict) -> set[str]:
    if "$ref" in prop:
        return json_types(defs[prop["$ref"].split("/")[-1]], defs)
    if "anyOf" in prop:
        return set().union(*(json_types(p, defs) for p in prop["anyOf"]))
    if "const" in prop or "enum" in prop:
        return {"string"}
    kind = prop["type"]
    return set(kind) if isinstance(kind, list) else {kind}


# ---------------- index templates ----------------

@pytest.mark.parametrize("family", FAMILIES)
def test_template_composes_shared_components(family):
    expected = ["movie_trend_base_settings", "movie_trend_batch_meta"]
    if family in ANALYTICS:
        expected.append("movie_trend_analytics_meta")
    tpl = template(family)
    assert tpl["composed_of"] == expected
    assert load_json(COMPONENTS_DIR / "base_settings.json")["template"]["mappings"]["dynamic"] == "strict"


@pytest.mark.parametrize("family", FAMILIES)
def test_template_fields_match_schema(family):
    props, sch = mapped_properties(template(family)), schema(family)
    assert set(props) == set(sch["properties"]), "template và schema phải có cùng tập field (mapping dynamic=strict)"
    for field, mapping in props.items():
        assert ES_TO_JSON[mapping["type"]] in json_types(sch["properties"][field], sch.get("$defs", {})), field
    assert props["title"]["fields"]["keyword"]["type"] == "keyword"  # contract mục 4: title.keyword


@pytest.mark.parametrize("family", FAMILIES)
def test_template_patterns_cover_canonical_physical_and_dev(family):
    patterns = template(family)["index_patterns"]
    for name in (family, f"{family}_20261024t104145z", f"{family}_fixture-p2-v1", f"dev_p2_{family}"):
        assert any(fnmatch.fnmatchcase(name, p) for p in patterns), name
    assert not any(p.startswith("dev_*") for p in patterns), "Huong_dan: không dùng dev_*_<bảng>*"


def test_lane_templates_do_not_overlap_other_templates():
    """ES từ chối hai index template cùng priority mà cùng khớp một tên index."""
    all_templates = {p.stem: load_json(p) for p in LANES_DIR.glob("*/templates/*.json")}
    samples = {name: [name, f"{name}_20261024t104145z", f"dev_p2_{name}", f"dev_p5_{name}"] for name in all_templates}
    for family in FAMILIES:
        mine = template(family)
        for other, tpl in all_templates.items():
            if other == family or tpl["priority"] != mine["priority"]:
                continue
            for name in samples[family] + samples[other]:
                hits = [any(fnmatch.fnmatchcase(name, p) for p in t["index_patterns"]) for t in (mine, tpl)]
                assert not all(hits), f"{family} và {other} cùng khớp {name}"


def test_templates_cover_spark_reader_and_writer_columns():
    readers = pytest.importorskip("modules.spark_sql.common.readers.es_reader")
    writers = pytest.importorskip("modules.spark_sql.common.writers.analytics_writer")
    for view, family in (("batch_movie", "movie_batch_stats"), ("batch_movie_daily", "movie_daily_stats")):
        assert set(readers.SOURCE_SCHEMAS[view].fieldNames()) <= set(mapped_properties(template(family)))
    for family in ANALYTICS:
        needed = set(writers.ANALYTICS_KEY_COLUMNS[family]) | set(writers.FAMILY_REQUIRED_COLUMNS[family])
        assert needed | set(writers.COMMON_ANALYTICS_COLUMNS) <= set(mapped_properties(template(family)))


# ---------------- fixture ----------------

def test_fixture_files_are_generated_by_script():
    proc = subprocess.run([sys.executable, str(LANE_DIR / "build_fixtures.py"), "--check"],
                          capture_output=True, text=True, cwd=ROOT)
    assert proc.returncode == 0, proc.stderr


@pytest.mark.parametrize("family", FAMILIES)
def test_fixture_rows_validate_and_have_unique_ids(family):
    validator = Draft202012Validator(schema(family), format_checker=FormatChecker())
    ids = set()
    for row in fixture(family):
        errors = [e.message for e in validator.iter_errors(row)]
        assert not errors, (row, errors)
        typed = {k: date.fromisoformat(row[k]) if k in ("date", "period_start") and row[k] else row.get(k)
                 for k in KEYS[family]}
        ids.add(build_record_id(typed, KEYS[family]))
    assert len(ids) == len(fixture(family)), "trùng _id trong fixture"


def test_fixture_metadata_is_one_run():
    manifest_hash = hashlib.sha256((FIXTURES_DIR / "manifest.json").read_bytes()).hexdigest()
    for family in FAMILIES:
        for row in fixture(family):
            assert (row["batch_run_id"], row["batch_cutoff_time"]) == ("fixture-p2-v1", "2018-03-01T00:00:00Z")
            if family in ANALYTICS:
                assert row["manifest_hash"] == manifest_hash
                assert row["ref_version"] == load_json(FIXTURES_DIR / "manifest.json")["reference"]["ref_version"]


def test_fixture_sources_are_consistent():
    daily, movies = fixture("movie_daily_stats"), {r["movieId"]: r for r in fixture("movie_batch_stats")}
    per_movie = defaultdict(lambda: [0, 0.0])
    for r in daily:
        assert r["interaction_count"] == r["rating_count"]
        assert r["avg_rating"] == pytest.approx(r["rating_sum"] / r["rating_count"])
        assert r["date"] < "2018-03-01", "source chỉ chứa event_time < T"
        per_movie[r["movieId"]][0] += r["rating_count"]
        per_movie[r["movieId"]][1] += r["rating_sum"]
    assert set(per_movie) == set(movies)
    for movie, (count, total) in per_movie.items():
        row = movies[movie]
        assert (row["total_count"], row["rating_count"]) == (count, count)
        assert row["rating_sum"] == pytest.approx(total) and row["avg_rating"] == pytest.approx(total / count)
    assert movies[99]["title"] is None, "fixture phải có ca thiếu title"


def expected_periods(period_type: str) -> dict:
    """(period_start, movieId) -> [count, sum], tự cộng lại từ fixture movie_daily_stats."""
    def start(day: str):
        return {"day": day, "month": day[:8] + "01", "year": day[:5] + "01-01", "all": None}[period_type]
    acc = defaultdict(lambda: [0, 0.0])
    for r in fixture("movie_daily_stats"):
        acc[(start(r["date"]), r["movieId"])][0] += r["rating_count"]
        acc[(start(r["date"]), r["movieId"])][1] += r["rating_sum"]
    return acc


@pytest.mark.parametrize("period_type", ["day", "month", "year", "all"])
def test_fixture_timeseries_matches_daily_source(period_type):
    rows = [r for r in fixture("analytics_timeseries") if r["period_type"] == period_type]
    titles = {r["movieId"]: r["title"] for r in fixture("movie_batch_stats")}
    expected = expected_periods(period_type)
    movie_rows = {(r["period_start"], r["movieId"]): r for r in rows if r["entity_type"] == "movie"}
    assert set(movie_rows) == set(expected)
    for key, (count, total) in expected.items():
        r = movie_rows[key]
        assert (r["interaction_count"], r["rating_count"]) == (count, count)
        assert r["rating_sum"] == pytest.approx(total) and r["avg_rating"] == pytest.approx(total / count)
        assert r["title"] == titles[key[1]]
    all_rows = {r["period_start"]: r for r in rows if r["entity_type"] == "all"}
    for start, r in all_rows.items():
        cells = [(c, s) for (p, _), (c, s) in expected.items() if p == start]
        assert r["rating_count"] == sum(c for c, _ in cells), "tổng dòng movie phải bằng dòng all"
        assert r["avg_rating"] == pytest.approx(sum(s for _, s in cells) / r["rating_count"]), "không avg-of-avg"
        assert r["distinct_movie_count"] == len(cells)
    assert set(all_rows) == {p for p, _ in expected}


def test_fixture_has_single_all_time_row_for_d1():
    rows = [r for r in fixture("analytics_timeseries") if r["period_type"] == "all" and r["entity_type"] == "all"]
    assert len(rows) == 1 and rows[0]["period_start"] is None
    daily = fixture("movie_daily_stats")
    assert rows[0]["rating_count"] == sum(r["rating_count"] for r in daily) == 12
    assert rows[0]["distinct_movie_count"] == len(fixture("movie_batch_stats")) == 4


def test_fixture_has_empty_month_before_cutoff():
    months = {r["period_start"] for r in fixture("analytics_timeseries") if r["period_type"] == "month"}
    assert "2018-02-01" not in months and {"2017-12-01", "2018-01-01"} <= months


@pytest.mark.parametrize("period_type", ["month", "year", "all"])
def test_fixture_top_movies_rank_every_movie_with_tie_break(period_type):
    rows = [r for r in fixture("analytics_top_movies") if r["period_type"] == period_type]
    expected = expected_periods(period_type)
    by_period = defaultdict(list)
    for r in rows:
        by_period[r["period_start"]].append(r)
    assert set(by_period) == {p for p, _ in expected}
    if period_type != "all":
        assert len(by_period) >= 2, "Top phải có ít nhất hai kỳ"
    for start, group in by_period.items():
        assert {r["movieId"] for r in group} == {m for p, m in expected if p == start}, "lưu đủ mọi phim trong kỳ"
        ordered = sorted(group, key=lambda r: r["rank_in_period"])
        assert [r["rank_in_period"] for r in ordered] == list(range(1, len(group) + 1))
        assert ordered == sorted(group, key=lambda r: (-r["interaction_count"], r["movieId"]))
    assert {r["period_type"] for r in fixture("analytics_top_movies")} == {"month", "year", "all"}


@pytest.mark.parametrize("family", FAMILIES)
def test_fixture_loader_ids_use_contract_keys_with_date_types(family):
    from modules.elasticsearch.lanes.movie_batch.load_fixtures import FAMILY_KEYS, build_records
    assert FAMILY_KEYS[family] == KEYS[family]
    rows = fixture(family)
    records = build_records(family, rows)
    assert [{k: v for k, v in r.items() if k != "_id"} for r in records] == rows
    for row, rec in zip(rows, records):
        typed = {k: date.fromisoformat(row[k]) if k in ("date", "period_start") and row[k] else row.get(k)
                 for k in KEYS[family]}
        assert rec["_id"] == build_record_id(typed, KEYS[family])


Q8_DIR = FIXTURES_DIR / "query8"
P4_RT_FIXTURE = ROOT / "contracts/phase2/movielens_realtime/fixtures/movie_realtime_stats.jsonl"


def q8(name: str) -> list[dict]:
    return [json.loads(line) for line in (Q8_DIR / f"{name}.jsonl").read_text(encoding="utf-8").splitlines()]


@pytest.mark.parametrize("family", SOURCES)
def test_query8_batch_fixture_validates(family):
    validator = Draft202012Validator(schema(family), format_checker=FormatChecker())
    manifest = load_json(Q8_DIR / "manifest.json")
    for row in q8(family):
        assert not [e.message for e in validator.iter_errors(row)], row
        assert (row["batch_run_id"], row["batch_cutoff_time"]) == (manifest["run_id"], manifest["batch_cutoff_time"])
    movies = {r["movieId"]: r for r in q8("movie_batch_stats")}
    for r in q8("movie_daily_stats"):
        assert r["rating_count"] == movies[r["movieId"]]["total_count"]  # mỗi phim chỉ một ngày


@pytest.mark.skipif(not P4_RT_FIXTURE.is_file(), reason="Chưa có fixture movielens_realtime của P4")
@pytest.mark.parametrize("size", ["10m", "1h"])
def test_query8_batch_side_matches_p4_realtime_fixture(size):
    """T nằm giữa hai nguồn: Batch (< T) bằng đúng các cửa sổ RT kết thúc trước T, Batch + RT (>= T) ra đủ mọi rating."""
    t = load_json(Q8_DIR / "manifest.json")["batch_cutoff_time"]
    windows = [json.loads(line) for line in P4_RT_FIXTURE.read_text(encoding="utf-8").splitlines()]
    windows = [w for w in windows if w["window_size"] == size]
    assert not [w for w in windows if w["window_start"] < t < w["window_end"]], "T phải tròn giờ, không cắt cửa sổ"
    before, after, every = defaultdict(int), defaultdict(int), defaultdict(int)
    for w in windows:
        every[w["movieId"]] += w["interaction_count"]
        (after if w["window_start"] >= t else before)[w["movieId"]] += w["interaction_count"]
    batch = {r["movieId"]: r["interaction_count"] for r in q8("movie_daily_stats")}
    assert batch == dict(before), "phía Batch phải đúng bằng phần RT trước T"
    merged = {m: batch.get(m, 0) + after.get(m, 0) for m in set(batch) | set(after)}
    assert merged == dict(every) and sum(merged.values()) == 23
    assert sum(batch.values()) + sum(every.values()) == 30, "cộng thẳng không lọc T sẽ đếm trùng 7 rating"


def test_fixture_top_movies_contains_ties():
    groups = defaultdict(list)
    for r in fixture("analytics_top_movies"):
        groups[(r["period_type"], r["period_start"], r["interaction_count"])].append(r["movieId"])
    assert any(len(m) > 1 for m in groups.values()), "fixture phải có ca hòa để thử tie-break"
