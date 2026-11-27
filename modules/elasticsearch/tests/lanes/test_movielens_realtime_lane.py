"""Lane movielens_realtime (P4): index templates, JSON Schema và fixture phải khớp nhau và khớp DATA_CONTRACT_Phase2.

Chạy offline, không cần Elasticsearch:
    .venv/bin/python -m pytest modules/elasticsearch/tests/lanes -q
Kỳ vọng được tính lại độc lập từ fixture source + contracts/samples/movie-reference.sample.tsv,
không dùng lại hàm của build_fixtures.py.
"""
from __future__ import annotations

import csv
import fnmatch
import json
import subprocess
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker

from modules.spark_sql.common.writers.key_serializer import build_record_id, serialize_key_v1

ROOT = Path(__file__).resolve().parents[4]
LANES_DIR = ROOT / "modules/elasticsearch/lanes"
TEMPLATES_DIR = LANES_DIR / "movielens_realtime/templates"
COMPONENTS_DIR = ROOT / "modules/elasticsearch/common/templates/components"
LANE_DIR = ROOT / "contracts/phase2/movielens_realtime"
FIXTURES_DIR = LANE_DIR / "fixtures"
REFERENCE_SAMPLE = ROOT / "contracts/samples/movie-reference.sample.tsv"

SOURCES = ("movie_realtime_stats", "genre_realtime")
ANALYTICS = ("analytics_growth", "analytics_growth_current")
FAMILIES = SOURCES + ANALYTICS
COMPONENTS = {
    "movie_realtime_stats": ["movie_trend_base_settings"],
    "genre_realtime": ["movie_trend_base_settings"],
    "analytics_growth": ["movie_trend_base_settings", "movie_trend_analytics_meta"],
    "analytics_growth_current": ["movie_trend_base_settings", "movie_trend_analytics_meta",
                                 "movie_trend_growth_current_meta"],
}
SOURCE_ENTITY = {"movie_realtime_stats": "movieId", "genre_realtime": "genre"}
SOURCE_TEMP_VIEW = {"movie_realtime_stats": "rt_movie", "genre_realtime": "rt_genre"}
SIZES = {"10m": timedelta(minutes=10), "1h": timedelta(hours=1)}
BATCH_CUTOFF = "2018-10-01T00:00:00Z"
# Cột business của growth được copy nguyên từ history sang current (contract 6: "toàn bộ business fields")
BUSINESS_FIELDS = ("window_start", "window_end", "window_size", "entity_type", "entity_key", "movieId", "genre",
                   "title", "title_source", "current_window_count", "previous_window_count",
                   "previous_window_present", "trending_score", "rating_count", "rating_sum", "avg_rating",
                   "updated_at", "timeline_mode")
ES_TO_JSON = {"integer": "integer", "long": "integer", "double": "number", "keyword": "string", "text": "string",
              "date": "string", "boolean": "boolean"}


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def template(family: str) -> dict:
    return load_json(TEMPLATES_DIR / f"{family}.json")


def mapped_properties(tpl: dict) -> dict:
    props: dict = {}
    for name in tpl["composed_of"]:
        comp = load_json(COMPONENTS_DIR / f"{name.removeprefix('movie_trend_')}.json")
        props.update(comp["template"].get("mappings", {}).get("properties", {}))
    props.update(tpl["template"]["mappings"]["properties"])
    return props


def schema(family: str) -> dict:
    return load_json(LANE_DIR / "schemas" / f"{family}.schema.json")


def read_jsonl(name: str) -> list[dict]:
    return [json.loads(line) for line in (FIXTURES_DIR / name).read_text(encoding="utf-8").splitlines()]


def fixture(family: str) -> list[dict]:
    return read_jsonl(f"{family}.jsonl")


def snapshots() -> list[dict]:
    return load_json(FIXTURES_DIR / "snapshots.json")["snapshots"]


def current(snap: dict) -> list[dict]:
    return read_jsonl(snap["fixture_file"])


def history_generation() -> dict:
    return load_json(FIXTURES_DIR / "snapshots.json")["history_generation"]


def ts(text: str) -> datetime:
    return datetime.strptime(text, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def fmt(value: datetime) -> str:
    return value.strftime("%Y-%m-%dT%H:%M:%SZ")


def reference() -> dict[int, dict]:
    with REFERENCE_SAMPLE.open(encoding="utf-8", newline="") as fh:
        return {int(r["movieId"]): r for r in csv.DictReader(fh, delimiter="\t")}


def json_types(prop: dict, defs: dict) -> set[str]:
    if "$ref" in prop:
        return json_types(defs[prop["$ref"].split("/")[-1]], defs)
    if "const" in prop or "enum" in prop:
        return {"string"}
    kind = prop["type"]
    return set(kind) if isinstance(kind, list) else {kind}


def snapshot_errors(rows: list[dict], history: list[dict], snap: dict) -> list[str]:
    """Oracle độc lập cho READY check của current snapshot (contract 6.2 bước 1-4)."""
    errors = []
    boundary = snap["source_complete_through"]
    for size in SIZES:
        complete = [h["window_start"] for h in history if h["window_size"] == size and h["window_end"] <= boundary]
        expected_w = max(complete)
        expected = {(h["entity_type"], h["entity_key"]): h for h in history
                    if h["window_size"] == size and h["window_start"] == expected_w}
        got = {(r["entity_type"], r["entity_key"]): r for r in rows if r["window_size"] == size}
        if not got:
            errors.append(f"thiếu size {size}")
            continue
        if {r["window_start"] for r in rows if r["window_size"] == size} != {expected_w}:
            errors.append(f"{size}: window khác W={expected_w}")
        if set(got) != set(expected):
            errors.append(f"{size}: tập entity khác history tại W")
        if {t for t, _ in got} != {"movie", "genre"}:
            errors.append(f"{size}: thiếu một entity type")
        for key, r in got.items():
            if key in expected and any(r[f] != expected[key][f] for f in BUSINESS_FIELDS):
                errors.append(f"{size} {key}: business field lệch history")
    keys = [(r["window_size"], r["entity_type"], r["entity_key"]) for r in rows]
    if len(keys) != len(set(keys)):
        errors.append("trùng khóa physical _id")
    for r in rows:
        if (r["snapshot_version"], r["source_complete_through"], r["history_analytics_run_id"]) != (
                snap["snapshot_version"], boundary, snap["history_analytics_run_id"]):
            errors.append("metadata snapshot không đồng nhất")
        if r["selected_window_start"] != r["window_start"] or r["window_end"] > boundary:
            errors.append("selected_window_start sai hoặc window chưa complete")
    return errors


# ---------------- index templates ----------------

@pytest.mark.parametrize("family", FAMILIES)
def test_template_composes_shared_components(family):
    assert template(family)["composed_of"] == COMPONENTS[family]
    assert load_json(COMPONENTS_DIR / "base_settings.json")["template"]["mappings"]["dynamic"] == "strict"


@pytest.mark.parametrize("family", FAMILIES)
def test_template_fields_match_schema(family):
    props, sch = mapped_properties(template(family)), schema(family)
    assert set(props) == set(sch["properties"]), "template và schema phải có cùng tập field (mapping dynamic=strict)"
    for field, mapping in props.items():
        assert ES_TO_JSON[mapping["type"]] in json_types(sch["properties"][field], sch.get("$defs", {})), field
    if family in ANALYTICS:
        assert props["title"]["fields"]["keyword"]["type"] == "keyword"  # contract mục 4: title.keyword
        assert props["previous_window_present"]["type"] == "boolean"


@pytest.mark.parametrize("family", SOURCES)
def test_source_template_equals_spark_streaming_output(family):
    """Spark Streaming bỏ doc_id rồi ghi đúng các cột này; dynamic=strict nên thừa/thiếu field đều làm Bulk lỗi."""
    readers = pytest.importorskip("modules.spark_sql.common.readers.es_reader")
    assert set(readers.SOURCE_SCHEMAS[SOURCE_TEMP_VIEW[family]].fieldNames()) == set(mapped_properties(template(family)))


def test_analytics_templates_cover_writer_columns():
    writers = pytest.importorskip("modules.spark_sql.common.writers.analytics_writer")
    for family in ANALYTICS:
        needed = (set(writers.ANALYTICS_KEY_COLUMNS[family]) | set(writers.FAMILY_REQUIRED_COLUMNS[family])
                  | set(writers.COMMON_ANALYTICS_COLUMNS))
        assert needed <= set(mapped_properties(template(family))), family


@pytest.mark.parametrize("family,names", [
    ("movie_realtime_stats", ["movie_realtime_stats", "dev_p4_movie_realtime_stats"]),
    ("genre_realtime", ["genre_realtime", "dev_p4_genre_realtime"]),
    ("analytics_growth", ["analytics_growth", "analytics_growth-g2", "dev_p4_analytics_growth"]),
    ("analytics_growth_current", ["analytics_growth_current-vfixture-p4-s002",
                                  "dev_p4_analytics_growth_current-vfixture-p4-s001"]),
])
def test_template_patterns_cover_canonical_physical_and_dev(family, names):
    patterns = template(family)["index_patterns"]
    for name in names:
        assert any(fnmatch.fnmatchcase(name, p) for p in patterns), name
    assert not any(p.startswith("dev_*") for p in patterns), "Huong_dan: không dùng dev_*_<bảng>*"


def test_growth_history_and_current_templates_never_match_same_index():
    """analytics_growth* sẽ nuốt analytics_growth_current*; ES từ chối 2 template cùng priority trùng pattern."""
    history, cur = template("analytics_growth")["index_patterns"], template("analytics_growth_current")["index_patterns"]
    samples = ["analytics_growth", "analytics_growth-g2", "analytics_growth_current", "analytics_growth_current-v1",
               "dev_p4_analytics_growth", "dev_p4_analytics_growth-x", "dev_p4_analytics_growth_current-vfixture-p4-s001"]
    for name in samples:
        assert sum(any(fnmatch.fnmatchcase(name, p) for p in pats) for pats in (history, cur)) == 1, name


def test_lane_templates_do_not_overlap_other_templates():
    all_templates = {p.stem: load_json(p) for p in LANES_DIR.glob("*/templates/*.json")}
    samples = {name: [name, f"{name}_20261024t104145z", f"{name}-vx", f"dev_p2_{name}", f"dev_p4_{name}",
                      f"dev_p4_{name}-vx"] for name in all_templates}
    for family in FAMILIES:
        mine = template(family)
        for other, tpl in all_templates.items():
            if other == family or tpl["priority"] != mine["priority"]:
                continue
            for name in samples[family] + samples[other]:
                hits = [any(fnmatch.fnmatchcase(name, p) for p in t["index_patterns"]) for t in (mine, tpl)]
                assert not all(hits), f"{family} và {other} cùng khớp {name}"


def test_roles_allow_p4_to_write_template_physical_names():
    roles = load_json(ROOT / "modules/elasticsearch/common/roles/phase2_roles.json")
    allowed = [n for block in roles["movie_trend_spark_sql_p4"]["indices"] for n in block["names"]]
    for name in ("analytics_growth", "analytics_growth-g2", "analytics_growth_current-vfixture-p4-s002",
                 "dev_p4_analytics_growth"):
        assert any(fnmatch.fnmatchcase(name, p) for p in allowed), name


# ---------------- fixture: chung ----------------

def test_fixture_files_are_generated_by_script():
    proc = subprocess.run([sys.executable, str(LANE_DIR / "build_fixtures.py"), "--check"],
                          capture_output=True, text=True, cwd=ROOT)
    assert proc.returncode == 0, proc.stderr


def all_fixture_rows() -> list[tuple[str, dict]]:
    rows = [(f, r) for f in ("movie_realtime_stats", "genre_realtime", "analytics_growth") for r in fixture(f)]
    return rows + [("analytics_growth_current", r) for s in snapshots() for r in current(s)]


@pytest.mark.parametrize("family", FAMILIES)
def test_fixture_rows_validate_against_schema(family):
    validator = Draft202012Validator(schema(family), format_checker=FormatChecker())
    rows = [r for f, r in all_fixture_rows() if f == family]
    assert rows
    for row in rows:
        errors = [e.message for e in validator.iter_errors(row)]
        assert not errors, (row, errors)


def test_fixture_is_replay_before_batch_cutoff():
    """Replay MovieLens hiện có đều trước T; fixture không giả làm dữ liệu sau T (query 8 thuộc K2)."""
    for _, r in all_fixture_rows():
        assert r["window_end"] <= BATCH_CUTOFF


# ---------------- fixture: source ----------------

@pytest.mark.parametrize("family", SOURCES)
def test_source_windows_are_aligned_half_open_and_unique(family):
    rows, keys = fixture(family), set()
    for r in rows:
        start, size = ts(r["window_start"]), SIZES[r["window_size"]]
        assert (start - datetime(1970, 1, 1, tzinfo=timezone.utc)) % size == timedelta(0), "window phải align UTC"
        assert ts(r["window_end"]) - start == size
        assert r["interaction_count"] == r["rating_count"]
        assert r["avg_rating"] == round(r["rating_sum"] / r["rating_count"], 4)  # giống F.round(...,4) Module 05
        keys.add((r[SOURCE_ENTITY[family]], r["window_start"], r["window_size"]))
    assert len(keys) == len(rows), "trùng grain (entity, window_start, window_size)"
    assert {r["window_size"] for r in rows} == set(SIZES)


@pytest.mark.parametrize("family", SOURCES)
def test_each_window_size_sums_to_same_total(family):
    """Contract mục 2: mỗi size cộng đủ cùng số rating/genre assignment; không cộng hai size với nhau."""
    totals = defaultdict(lambda: [0, 0.0])
    for r in fixture(family):
        totals[r["window_size"]][0] += r["rating_count"]
        totals[r["window_size"]][1] += r["rating_sum"]
    assert totals["10m"][0] == totals["1h"][0]
    assert totals["10m"][1] == pytest.approx(totals["1h"][1])


def test_genre_source_equals_movie_source_exploded_by_reference():
    ref = reference()
    expected = defaultdict(int)
    for r in fixture("movie_realtime_stats"):
        raw = ref.get(r["movieId"], {}).get("genres")
        genres = ["UNKNOWN"] if not raw or raw == "(no genres listed)" else raw.split("|")
        for g in genres:
            expected[(r["window_size"], r["window_start"], g)] += r["interaction_count"]
    got = {(r["window_size"], r["window_start"], r["genre"]): r["interaction_count"] for r in fixture("genre_realtime")}
    assert got == dict(expected)
    assert any(g == "UNKNOWN" for _, _, g in got), "fixture phải có genre UNKNOWN (join miss)"


# ---------------- fixture: analytics_growth (history) ----------------

def test_history_has_one_row_per_source_window_with_exact_previous():
    sources = {"movie": (fixture("movie_realtime_stats"), "movieId"), "genre": (fixture("genre_realtime"), "genre")}
    history = {(r["window_size"], r["window_start"], r["entity_type"], r["entity_key"]): r
               for r in fixture("analytics_growth")}
    expected_keys = set()
    for entity_type, (rows, field) in sources.items():
        index = {(r["window_size"], r["window_start"], str(r[field])): r for r in rows}
        for r in rows:
            key = (r["window_size"], r["window_start"], entity_type, str(r[field]))
            expected_keys.add(key)
            h = history[key]
            prev = index.get((r["window_size"], fmt(ts(r["window_start"]) - SIZES[r["window_size"]]), str(r[field])))
            assert h["current_window_count"] == r["interaction_count"]
            assert (h["rating_count"], h["rating_sum"], h["avg_rating"]) == (r["rating_count"], r["rating_sum"],
                                                                           r["avg_rating"])
            assert h["window_end"] == r["window_end"] and h["updated_at"] == r["updated_at"]
            if prev is None:
                assert (h["previous_window_present"], h["previous_window_count"], h["trending_score"]) == (
                    False, None, None), "thiếu previous -> null, không phải 0"
            else:
                c, p = r["interaction_count"], prev["interaction_count"]
                assert h["previous_window_present"] is True and h["previous_window_count"] == p
                assert h["trending_score"] == pytest.approx((c - p) / (p + 1))
    assert set(history) == expected_keys, "history phải đủ và chỉ gồm các source window"
    assert len(fixture("analytics_growth")) == len(expected_keys)


def test_history_previous_is_adjacent_not_nearest():
    """Có ca entity có window cũ hơn nhưng KHÔNG liền kề -> previous vẫn phải missing."""
    rows = fixture("analytics_growth")
    starts = defaultdict(set)
    for r in rows:
        starts[(r["window_size"], r["entity_type"], r["entity_key"])].add(r["window_start"])
    gaps = [r for r in rows if not r["previous_window_present"]
            and any(s < r["window_start"] for s in starts[(r["window_size"], r["entity_type"], r["entity_key"])])]
    assert {r["window_size"] for r in gaps} == set(SIZES), "mỗi size cần một ca previous không liền kề"


def test_missing_previous_is_null_never_zero():
    """Mô hình source hiện tại: window chỉ tồn tại khi có >= 1 rating. Kỳ trước không có rating = thiếu = null.

    Ca contract mục 5 "previous tồn tại với count thật = 0" không thể sinh từ source canonical nên không có trong
    fixture; schema chặn previous_window_count = 0 để không ai tự đổi null thành 0.
    """
    for r in fixture("analytics_growth") + [r for s in snapshots() for r in current(s)]:
        if r["previous_window_present"]:
            assert r["previous_window_count"] >= 1 and r["trending_score"] is not None
        else:
            assert r["previous_window_count"] is None and r["trending_score"] is None
    for family in ANALYTICS:
        validator = Draft202012Validator(schema(family), format_checker=FormatChecker())
        base = next(r for r in (fixture("analytics_growth") if family == "analytics_growth"
                                else current(snapshots()[-1])) if not r["previous_window_present"])
        coerced_zero = {**base, "previous_window_count": 0, "trending_score": float(base["current_window_count"])}
        assert list(validator.iter_errors(coerced_zero)), f"{family}: present=false mà previous=0 phải bị từ chối"
        assert list(validator.iter_errors({**coerced_zero, "previous_window_present": True})), \
            f"{family}: previous_window_count=0 không hợp lệ với mô hình source hiện tại"


def test_history_covers_score_sign_and_zero_cases():
    scores = [r["trending_score"] for r in fixture("analytics_growth")]
    assert any(s is None for s in scores)
    assert any(s == 0.0 for s in scores if s is not None), "score=0 phải khác null"
    assert any(s > 0 for s in scores if s is not None) and any(s < 0 for s in scores if s is not None)


def test_history_title_enrichment_is_label_only_with_fallback():
    ref = reference()
    for r in fixture("analytics_growth"):
        if r["entity_type"] == "movie":
            assert r["entity_key"] == str(r["movieId"]) and r["genre"] is None
            title = ref.get(r["movieId"], {}).get("title")
            assert r["title"] == title
            assert r["title_source"] == (f"movie_reference/{ref[r['movieId']]['ref_version']}" if title else None)
        else:
            assert r["entity_key"] == r["genre"] and r["movieId"] is None and r["title"] is None
    assert any(r["entity_type"] == "movie" and r["title"] is None for r in fixture("analytics_growth")), \
        "cần ca thiếu title để D4 fallback entity_key"


def test_history_has_incomplete_window_newer_than_every_boundary():
    latest_boundary = max(s["source_complete_through"] for s in snapshots())
    for size in SIZES:
        assert any(r["window_size"] == size and r["window_end"] > latest_boundary
                   for r in fixture("analytics_growth")), f"{size} cần một window chưa complete"


# ---------------- fixture: analytics_growth_current ----------------

def test_snapshot_manifest_lists_only_published_ready_versions():
    """Artefact đã publish chỉ gồm snapshot READY; snapshot lỗi không bao giờ có published_at (contract 7.2)."""
    snaps = snapshots()
    assert len(snaps) >= 2, "contract mục 9: hai physical current versions"
    assert all(s["status"] == "READY" for s in snaps)
    for s in snaps:
        assert s["physical_index"] == f"analytics_growth_current-v{s['snapshot_version']}"
        assert (FIXTURES_DIR / s["fixture_file"]).is_file()
        assert {r["published_at"] for r in current(s)} == {s["published_at"]}
    assert len({s["snapshot_version"] for s in snaps}) == len(snaps)
    listed = {s["fixture_file"] for s in snaps}
    assert {p.name for p in FIXTURES_DIR.glob("analytics_growth_current.*.jsonl")} == listed, \
        "không có file current nào ngoài các snapshot đã publish"


def test_snapshots_reference_existing_history_generation_published_before_them():
    """history_analytics_run_id phải là generation có thật trong fixture và đã publish TRƯỚC snapshot."""
    gen = history_generation()
    history = fixture("analytics_growth")
    assert {r["analytics_run_id"] for r in history} == {gen["analytics_run_id"]}
    assert {r["published_at"] for r in history} == {gen["published_at"]}
    assert max(r["updated_at"] for r in history) <= gen["published_at"], "history không được publish trước source"
    previous_published = gen["published_at"]
    for snap in snapshots():  # thứ tự trong manifest = thứ tự publish
        assert snap["history_analytics_run_id"] == gen["analytics_run_id"]
        assert {r["history_analytics_run_id"] for r in current(snap)} == {gen["analytics_run_id"]}
        assert snap["published_at"] > previous_published, f"{snap['snapshot_version']} dùng dữ liệu từ tương lai"
        previous_published = snap["published_at"]


@pytest.mark.parametrize("snap", snapshots(), ids=lambda s: s["snapshot_version"])
def test_ready_snapshot_passes_complete_boundary_protocol(snap):
    assert snapshot_errors(current(snap), fixture("analytics_growth"), snap) == []


@pytest.mark.parametrize("snap", snapshots(), ids=lambda s: s["snapshot_version"])
def test_ready_snapshot_has_exactly_one_window_per_size(snap):
    rows = current(snap)
    for size in SIZES:
        assert len({r["window_start"] for r in rows if r["window_size"] == size}) == 1


def _incomplete_window(rows: list[dict]) -> list[dict]:
    """Thay 10m bằng window mới hơn nhưng chưa complete lấy từ history."""
    boundary = rows[0]["source_complete_through"]
    newer = [h for h in fixture("analytics_growth") if h["window_size"] == "10m" and h["window_end"] > boundary]
    meta = {k: rows[0][k] for k in ("schema_version", "analytics_run_id", "published_at", "snapshot_version",
                                    "source_complete_through", "history_analytics_run_id")}
    return [r for r in rows if r["window_size"] != "10m"] + [
        {**h, **meta, "selected_window_start": h["window_start"]} for h in newer]


BROKEN_SNAPSHOTS = {
    "missing_size_1h": lambda rows: [r for r in rows if r["window_size"] != "1h"],
    "missing_entity_genre": lambda rows: [r for r in rows if r["entity_type"] != "genre"],
    "missing_one_entity": lambda rows: rows[1:],
    "duplicate_key": lambda rows: rows + rows[:1],
    "incomplete_window_selected": _incomplete_window,
    "business_field_drift": lambda rows: [{**rows[0], "current_window_count": rows[0]["current_window_count"] + 1}]
    + rows[1:],
    "wrong_selected_window_start": lambda rows: [{**rows[0], "selected_window_start": "2018-09-20T11:00:00Z"}]
    + rows[1:],
}


@pytest.mark.parametrize("case", sorted(BROKEN_SNAPSHOTS))
def test_broken_candidate_built_from_valid_snapshot_is_rejected(case):
    """Ca lỗi (contract 6.2 bước 3-4, mục 12) dựng ngay trong test từ snapshot hợp lệ mới nhất; không lưu thành file."""
    snap = snapshots()[-1]
    candidate = BROKEN_SNAPSHOTS[case]([dict(r) for r in current(snap)])
    assert snapshot_errors(candidate, fixture("analytics_growth"), snap), case


def test_ready_versions_differ_so_rollback_is_observable():
    a, b = snapshots()[:2]
    pick = lambda s: {(r["window_size"], r["window_start"]) for r in current(s)}  # noqa: E731
    assert a["source_complete_through"] < b["source_complete_through"]
    assert pick(a).isdisjoint(pick(b)), "hai version phải trỏ window khác nhau để thấy rõ khi rollback"


def test_current_ids_are_stable_across_versions():
    """Physical _id = size|type|key: cùng entity có cùng _id ở mọi version, khác nhau giữa movie và genre."""
    from modules.elasticsearch.lanes.movielens_realtime.load_fixtures import build_records
    by_key = defaultdict(set)
    for snap in snapshots():
        for rec in build_records("analytics_growth_current", current(snap)):
            by_key[(rec["window_size"], rec["entity_type"], rec["entity_key"])].add(rec["_id"])
    assert all(len(ids) == 1 for ids in by_key.values())


# ---------------- loader ----------------

def test_loader_source_ids_match_spark_streaming_literal_format():
    from modules.elasticsearch.lanes.movielens_realtime.load_fixtures import build_records
    for rec in build_records("movie_realtime_stats", fixture("movie_realtime_stats")):
        assert rec["_id"] == f"{rec['movieId']}|{rec['window_start']}|{rec['window_size']}"
    for rec in build_records("genre_realtime", fixture("genre_realtime")):
        assert rec["_id"] == f"{rec['genre']}|{rec['window_start']}|{rec['window_size']}"


def test_loader_analytics_ids_match_shared_spark_writer():
    """_id phải giống writer Spark SQL: window_start là TimestampType -> datetime UTC trước khi băm."""
    writers = pytest.importorskip("modules.spark_sql.common.writers.analytics_writer")
    types = pytest.importorskip("pyspark.sql.types")
    from modules.elasticsearch.lanes.movielens_realtime.load_fixtures import ANALYTICS_KEYS, build_records
    for family in ANALYTICS:
        assert ANALYTICS_KEYS[family] == writers.ANALYTICS_KEY_COLUMNS[family]
    rows = fixture("analytics_growth")
    for row, rec in zip(rows, build_records("analytics_growth", rows)):
        key = tuple(writers._to_key_value(row[k], types.TimestampType() if k == "window_start" else types.StringType())
                    for k in writers.ANALYTICS_KEY_COLUMNS["analytics_growth"])
        assert rec["_id"] == serialize_key_v1(key)
        assert rec["_id"] != build_record_id(row, ANALYTICS_KEYS["analytics_growth"]), "không được băm window_start dạng chuỗi"


def test_loader_plan_loads_every_published_snapshot_into_dev_namespace():
    from modules.elasticsearch.lanes.movielens_realtime.load_fixtures import load_plan
    plan = load_plan()
    names = [index for _, index, _ in plan]
    assert all(n.startswith("dev_p4_") for n in names)
    assert {f"dev_p4_{s['physical_index']}" for s in snapshots()} <= set(names)
    for _, index, records in plan:
        assert [{k: v for k, v in r.items() if k != "_id"} for r in records]
        family = next(f for f in FAMILIES if index == f"dev_p4_{f}" or index.startswith(f"dev_p4_{f}-v"))
        assert any(fnmatch.fnmatchcase(index, p) for p in template(family)["index_patterns"]), index
