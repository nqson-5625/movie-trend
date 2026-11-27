"""READY report của lane movie_batch (P2.3): kiểm tra ES trước khi READY và tương thích với publisher dùng chung.

Chạy offline với ES giả:
    .venv/bin/python -m pytest modules/elasticsearch/tests/lanes -q
"""
from __future__ import annotations

import copy

import pytest

from modules.elasticsearch.common.publication.ready_report import (
    IndexReadyEntry,
    ReadyReport,
    load_ready_report,
    save_ready_report,
    validate_ready_reports_pair,
)
from modules.elasticsearch.lanes.movie_batch.emit_ready_report import P2_ALIASES, build_ready_report, verify_in_es

RUN, T, REF, SHA = "20261024T104145Z", "2018-10-01T00:00:00Z", "20261017-5f26995f", "a" * 64
META = {"batch_run_id": RUN, "batch_cutoff_time": T, "ref_version": REF, "manifest_sha256": SHA}
ROWS = {"movie_batch_stats": 9724, "movie_daily_stats": 99538, "analytics_timeseries": 237096,
        "analytics_top_movies": 133157}
ENTRIES = {a: {"alias_name": a, "physical_index": f"{a}_{RUN.lower()}", "row_count": n, "bulk_error_count": 0}
           for a, n in ROWS.items()}


class FakeES:
    """Mỗi physical index: số document, giá trị của các field metadata và tổng lượt all-time."""

    def __init__(self):
        self.indices = {}
        for alias, n in ROWS.items():
            fields = {"batch_run_id": [(RUN, n)], "batch_cutoff_time": [("2018-10-01T00:00:00.000Z", n)]}
            if alias.startswith("analytics_"):
                fields.update(ref_version=[(REF, n)], manifest_hash=[(SHA, n)])
            self.indices[f"{alias}_{RUN.lower()}"] = {"count": n, "fields": fields, "total": 100836.0}

    def index_exists(self, index):
        return index in self.indices

    def count(self, index):
        return self.indices[index]["count"]

    def search(self, index, body):
        agg = body["aggs"]
        if "v" in agg:
            values = self.indices[index]["fields"][agg["v"]["terms"]["field"]]
            return {"aggregations": {"v": {"buckets": [{"key": v, "key_as_string": v, "doc_count": n}
                                                       for v, n in values]}}}
        return {"aggregations": {"s": {"value": self.indices[index]["total"]}}}


def p3_report(**overrides) -> ReadyReport:
    base = dict(owner="p3", schema_version="phase2-3.0", manifest_sha256=SHA, batch_run_id=RUN,
                batch_cutoff_time=T, ref_version=REF, sql_test_result="PASS", completed_at=T, status="READY",
                indices=[IndexReadyEntry("genre_trends", f"genre_trends_{RUN.lower()}", 32894, 0, 0, 274480.0),
                         IndexReadyEntry("analytics_genre_trends", f"analytics_genre_trends_{RUN.lower()}", 1, 0, 0)])
    return ReadyReport(**{**base, **overrides})


def test_ready_report_round_trips_and_pairs_with_p3(tmp_path):
    totals = verify_in_es(FakeES(), META, ENTRIES)
    report = build_ready_report(META, ENTRIES, totals, "PASS", completed_at="2026-11-27T00:00:00Z")
    assert report.status == "READY" and [e.alias_name for e in report.indices] == list(P2_ALIASES)
    loaded = load_ready_report(save_ready_report(report, tmp_path / f"p2_ready_{RUN}.json"))
    assert loaded == report
    paired = validate_ready_reports_pair(loaded, p3_report())  # đúng hàm P5 dùng trước khi publish 6 alias
    assert set(paired) == set(P2_ALIASES) | {"genre_trends", "analytics_genre_trends"}


def test_pairing_rejects_p3_with_other_manifest():
    report = build_ready_report(META, ENTRIES, verify_in_es(FakeES(), META, ENTRIES), "PASS")
    with pytest.raises(ValueError, match="manifest_sha256"):
        validate_ready_reports_pair(report, p3_report(manifest_sha256="b" * 64))


def test_failed_sql_tests_are_never_ready():
    report = build_ready_report(META, ENTRIES, verify_in_es(FakeES(), META, ENTRIES), "FAIL")
    assert report.status == "NOT_READY"


def broken(mutate):
    es = FakeES()
    mutate(es.indices)
    return es


@pytest.mark.parametrize("case, mutate, message", [
    ("thiếu index", lambda ix: ix.pop(f"analytics_top_movies_{RUN.lower()}"), "chưa tồn tại"),
    ("lệch số document", lambda ix: ix[f"movie_daily_stats_{RUN.lower()}"].update(count=99537), "99537"),
    ("lẫn hai run", lambda ix: ix[f"movie_batch_stats_{RUN.lower()}"]["fields"].update(
        batch_run_id=[(RUN, 9000), ("20261028T150122Z", 724)]), "batch_run_id"),
    ("analytics khác manifest", lambda ix: ix[f"analytics_timeseries_{RUN.lower()}"]["fields"].update(
        manifest_hash=[("b" * 64, 237096)]), "manifest_hash"),
    ("analytics khác ref_version", lambda ix: ix[f"analytics_top_movies_{RUN.lower()}"]["fields"].update(
        ref_version=[("20261010-01e56f1d", 133157)]), "ref_version"),
    ("tổng lượt lệch", lambda ix: ix[f"analytics_timeseries_{RUN.lower()}"].update(total=100835.0), "Tổng lượt"),
])
def test_verify_in_es_blocks_ready(case, mutate, message):
    with pytest.raises(ValueError, match=message):
        verify_in_es(broken(mutate), META, copy.deepcopy(ENTRIES))
