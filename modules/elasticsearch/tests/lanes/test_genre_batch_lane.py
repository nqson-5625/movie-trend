"""Lane genre_batch (P3): index templates, JSON Schema và fixture phải khớp nhau và khớp DATA_CONTRACT_Phase2.

Chạy offline, không cần Elasticsearch:
    .venv/bin/python -m pytest modules/elasticsearch/tests/lanes/test_genre_batch_lane.py -q
Fixture được kiểm bằng cách tự cộng lại từ genre_trends, độc lập với build_fixtures.py.
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
TEMPLATES_DIR = LANES_DIR / "genre_batch/templates"
COMPONENTS_DIR = ROOT / "modules/elasticsearch/common/templates/components"
LANE_DIR = ROOT / "contracts/phase2/genre_batch"
FIXTURES_DIR = LANE_DIR / "fixtures"

SOURCES = ("genre_trends",)
ANALYTICS = ("analytics_genre_trends",)
FAMILIES = SOURCES + ANALYTICS
KEYS = {
    "genre_trends": ("batch_run_id", "date", "genre"),
    "analytics_genre_trends": ("batch_run_id", "period_type", "period_start", "genre"),
}
ES_TO_JSON = {
    "integer": "integer",
    "long": "integer",
    "double": "number",
    "keyword": "string",
    "text": "string",
    "date": "string",
}


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
    return [json.loads(line) for line in lines if line.strip()]


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
    assert props["genre"]["type"] == "keyword"
    assert props["genre"]["fields"]["text"]["type"] == "text"


@pytest.mark.parametrize("family", FAMILIES)
def test_template_patterns_cover_canonical_physical_and_dev(family):
    patterns = template(family)["index_patterns"]
    for name in (family, f"{family}_20261024t104145z", f"{family}_fixture-p3-v1", f"dev_p3_{family}"):
        assert any(fnmatch.fnmatchcase(name, p) for p in patterns), name
    assert not any(p.startswith("dev_*") for p in patterns), "Huong_dan: không dùng dev_*_<bảng>*"


def test_genre_lane_templates_do_not_overlap_other_templates():
    """ES từ chối hai index template cùng priority mà cùng khớp một tên index."""
    all_templates = {p.stem: load_json(p) for p in LANES_DIR.glob("*/templates/*.json")}
    samples = {
        name: [name, f"{name}_20261024t104145z", f"dev_p2_{name}", f"dev_p3_{name}", f"dev_p5_{name}"]
        for name in all_templates
    }
    for family in FAMILIES:
        mine = template(family)
        for other, tpl in all_templates.items():
            if other == family or tpl["priority"] != mine["priority"]:
                continue
            for name in samples[family] + samples[other]:
                hits = [any(fnmatch.fnmatchcase(name, p) for p in t["index_patterns"]) for t in (mine, tpl)]
                assert not all(hits), f"{family} và {other} cùng khớp {name}"


def test_genre_templates_cover_spark_reader_and_writer_columns():
    readers = pytest.importorskip("modules.spark_sql.common.readers.es_reader")
    writers = pytest.importorskip("modules.spark_sql.common.writers.analytics_writer")
    assert set(readers.SOURCE_SCHEMAS["batch_genre"].fieldNames()) <= set(mapped_properties(template("genre_trends")))
    needed = set(writers.ANALYTICS_KEY_COLUMNS["analytics_genre_trends"]) | set(
        writers.FAMILY_REQUIRED_COLUMNS["analytics_genre_trends"]
    )
    assert needed | set(writers.COMMON_ANALYTICS_COLUMNS) <= set(mapped_properties(template("analytics_genre_trends")))


# ---------------- fixture ----------------


def test_genre_fixture_files_are_generated_by_script():
    proc = subprocess.run(
        [sys.executable, str(LANE_DIR / "build_fixtures.py"), "--check"],
        capture_output=True,
        text=True,
        cwd=ROOT,
    )
    assert proc.returncode == 0, proc.stderr


@pytest.mark.parametrize("family", FAMILIES)
def test_genre_fixture_rows_validate_and_have_unique_ids(family):
    validator = Draft202012Validator(schema(family), format_checker=FormatChecker())
    ids = set()
    for row in fixture(family):
        errors = [e.message for e in validator.iter_errors(row)]
        assert not errors, (row, errors)
        typed = {
            k: date.fromisoformat(row[k]) if k in ("date", "period_start") and row[k] else row.get(k)
            for k in KEYS[family]
        }
        ids.add(build_record_id(typed, KEYS[family]))
    assert len(ids) == len(fixture(family)), "trùng _id trong fixture"


def test_genre_fixture_metadata_is_one_run():
    manifest_hash = hashlib.sha256((FIXTURES_DIR / "manifest.json").read_bytes().replace(b"\r\n", b"\n")).hexdigest()
    for family in FAMILIES:
        for row in fixture(family):
            assert (row["batch_run_id"], row["batch_cutoff_time"]) == ("fixture-p3-v1", "2018-03-01T00:00:00Z")
            if family in ANALYTICS:
                assert row["manifest_hash"] == manifest_hash
                assert row["ref_version"] == load_json(FIXTURES_DIR / "manifest.json")["reference"]["ref_version"]


def test_genre_fixture_sources_keep_unknown_and_cutoff():
    rows = fixture("genre_trends")
    assert len(rows) >= 4
    genres = {r["genre"] for r in rows}
    assert "UNKNOWN" in genres, "contract mục 4/9: phải giữ UNKNOWN"
    assert len(genres) >= 2
    for r in rows:
        assert r["interaction_count"] == r["rating_count"]
        assert r["avg_rating"] == pytest.approx(r["rating_sum"] / r["rating_count"])
        assert r["date"] < "2018-03-01", "source chỉ chứa event_time < T"


def expected_genre_periods(period_type: str) -> dict[tuple[str, str], list[float]]:
    """(period_start, genre) -> [count, sum], tự cộng lại từ fixture genre_trends."""

    def start(day: str) -> str:
        return {"day": day, "month": day[:8] + "01", "year": day[:5] + "01-01"}[period_type]

    acc: dict[tuple[str, str], list[float]] = defaultdict(lambda: [0, 0.0])
    for r in fixture("genre_trends"):
        acc[(start(r["date"]), r["genre"])][0] += r["rating_count"]
        acc[(start(r["date"]), r["genre"])][1] += r["rating_sum"]
    return acc


@pytest.mark.parametrize("period_type", ["day", "month", "year"])
def test_genre_fixture_analytics_matches_source_and_tie_break(period_type):
    rows = [r for r in fixture("analytics_genre_trends") if r["period_type"] == period_type]
    expected = expected_genre_periods(period_type)
    actual_map = {(r["period_start"], r["genre"]): r for r in rows}
    assert set(actual_map) == set(expected)

    by_period = defaultdict(list)
    for (start, genre), (count, total) in expected.items():
        r = actual_map[(start, genre)]
        assert (r["interaction_count"], r["rating_count"]) == (count, count)
        assert r["rating_sum"] == pytest.approx(total)
        assert r["avg_rating"] == pytest.approx(total / count), "không avg-of-avg"
        by_period[start].append(r)

    assert len(by_period) >= 2, "contract mục 9: phải có ít nhất hai kỳ"
    for group in by_period.values():
        ordered = sorted(group, key=lambda r: r["rank_in_period"])
        assert [r["rank_in_period"] for r in ordered] == list(range(1, len(group) + 1))
        assert ordered == sorted(group, key=lambda r: (-r["interaction_count"], r["genre"]))


def test_genre_fixture_contract_edge_cases():
    rows = fixture("analytics_genre_trends")
    assert {r["period_type"] for r in rows} == {"day", "month", "year"}, "D3 không dùng period_type=all"
    months = {r["period_start"] for r in rows if r["period_type"] == "month"}
    assert "2018-02-01" not in months and {"2017-12-01", "2018-01-01"} <= months

    # Kiểm tra ca hòa hạng (tie-break theo genre ASC)
    groups = defaultdict(list)
    for r in rows:
        groups[(r["period_type"], r["period_start"], r["interaction_count"])].append(r["genre"])
    assert any(len(gs) > 1 for gs in groups.values()), "fixture phải có ca hòa để thử tie-break"


@pytest.mark.parametrize("family", FAMILIES)
def test_genre_fixture_loader_ids_use_contract_keys_with_date_types(family):
    from modules.elasticsearch.lanes.genre_batch.load_fixtures import FAMILY_KEYS, build_records

    assert FAMILY_KEYS[family] == KEYS[family]
    rows = fixture(family)
    records = build_records(family, rows)
    assert [{k: v for k, v in r.items() if k != "_id"} for r in records] == rows
    for row, rec in zip(rows, records):
        typed = {
            k: date.fromisoformat(row[k]) if k in ("date", "period_start") and row[k] else row.get(k)
            for k in KEYS[family]
        }
        assert rec["_id"] == build_record_id(typed, KEYS[family])
