"""Offline audit of Module 01 invariants, real fixtures and handoff artifacts."""
from __future__ import annotations

import csv
import io
import json
import os
import subprocess

import pandas as pd

from modules.data_source.src import build_movie_reference as reference
from modules.data_source.src import sort_ratings
from modules.data_source.src.build_manifest import file_metadata, verify_raw_sources, snapshot_metadata
from modules.data_source.src.contract_validation import (
    PROJECT_ROOT, atomic_write_text, load_validator, read_jsonl, read_reference,
    reference_record, validate_record, validate_reference_rows,
)
from modules.data_source.src.movie_event_adapter import to_movie_event
from modules.data_source.src.tmdb_source_adapter import utc_now


def audit_module() -> dict:
    root = PROJECT_ROOT
    manifest = json.loads((root / "data/reports/source_manifest.json").read_text(encoding="utf-8"))
    variant = manifest["movielens_variant"]
    utc_now_value = utc_now()
    checks = []

    def passed(name: str, **details) -> None:
        checks.append({"name": name, "status": "PASS", **details})
        print(f"[PASS] {name}", flush=True)

    sources = verify_raw_sources(manifest, root, variant)
    passed("raw_source_checksums", files=sum(len(source["files"]) for source in sources if source["source_name"] in {"MovieLens", "IMDb"}))
    for item in [manifest["reference"], *manifest["prepared"], *manifest["fixtures"]]:
        actual = file_metadata(root / item["path"], root)
        if actual["sha256"] != item["sha256"] or actual["size_bytes"] != item["size_bytes"]:
            raise ValueError(f"Manifest no longer describes artifact: {item['path']}")
    passed("prepared_and_fixture_checksums")

    for filename in ("data_profile.json", "data_quality_report.json"):
        report = json.loads((root / "data/reports" / filename).read_text(encoding="utf-8"))
        if report["status"] != "PASS" or report["hard_failures"]:
            raise ValueError(f"{filename} contains hard failures")
    passed("profiling_and_quality_reports")

    reference_path = root / manifest["reference"]["path"]
    rows = read_reference(reference_path)
    validate_reference_rows(rows)
    if len(rows) != manifest["reference"]["row_count"]:
        raise ValueError("Reference manifest row count is incorrect")
    actual_by_id = {int(row["movieId"]): reference_record(row) for row in rows}
    source_hash = reference.calculate_reference_source_hash(manifest)
    if rows[0]["ref_version"] != reference.build_ref_version(source_hash, rows[0]["generated_at"]):
        raise ValueError("Reference version does not describe current raw sources")
    # Rebuild in memory, keeping the existing build metadata; canonical stays intact.
    joined = reference.build_joined_reference()
    expected = reference.build_canonical_reference(joined, rows[0]["ref_version"], rows[0]["generated_at"])
    expected_rows = csv.DictReader(io.StringIO(expected.to_csv(sep="\t", index=False, na_rep="", lineterminator="\n")), delimiter="\t")
    expected_by_id = {int(row["movieId"]): reference_record(row) for row in expected_rows}
    if actual_by_id != expected_by_id:
        raise ValueError("Reference content differs from a fresh LEFT JOIN of verified raw sources")
    passed("canonical_reference_rebuilt_from_sources", rows=len(rows), ref_version=rows[0]["ref_version"])

    source = sort_ratings.load_ratings()
    sorted_df = sort_ratings.load_written_output()
    sort_ratings.validate_output(source, sorted_df)
    if len(sorted_df) != manifest["prepared"][0]["row_count"]:
        raise ValueError("Sorted ratings manifest row count is incorrect")
    validator = load_validator("movie-events")
    for row in sorted_df.to_dict(orient="records"):
        validate_record(to_movie_event(row, ingest_time=utc_now_value), validator)
    passed("all_ratings_replay_contract_and_sort", rows=len(sorted_df), min_timestamp=int(sorted_df.timestamp.astype(int).min()), max_timestamp=int(sorted_df.timestamp.astype(int).max()))

    tmdb_source = next(source for source in sources if source["source_name"] == "TMDB")
    reference_tmdb_ids = {row["tmdbId"] for row in actual_by_id.values() if row["tmdbId"] is not None}
    tmdb_record_count = 0
    tmdb_matched_count = 0
    for item in tmdb_source["files"]:
        actual = snapshot_metadata(root / item["path"], root)
        if actual["sha256"] != item["sha256"] or actual["row_count"] != item["row_count"]:
            raise ValueError("TMDB snapshot metadata mismatch")
        snapshot_records = read_jsonl(root / item["path"])
        tmdb_record_count += len(snapshot_records)
        tmdb_matched_count += sum(record["tmdbId"] in reference_tmdb_ids for record in snapshot_records)
    passed("all_tmdb_snapshots", snapshots=len(tmdb_source["files"]), records=tmdb_record_count, reference_matches=tmdb_matched_count)

    sample_dir = root / "contracts/samples"
    provenance = json.loads((sample_dir / "fixture_provenance.json").read_text(encoding="utf-8"))
    for item in provenance["fixtures"].values():
        if reference.sha256_file(root / item["source_path"]) != item["source_sha256"]:
            raise ValueError("Fixture provenance source has changed")
    events = read_jsonl(sample_dir / "movie-events.sample.jsonl")
    expected_events = [to_movie_event(row, provenance["generated_at"]) for row in sorted_df.head(len(events)).to_dict(orient="records")]
    if events != expected_events:
        raise ValueError("Movie event samples do not match real source rows")
    tmdb_sample = read_jsonl(sample_dir / "tmdb-snapshots.sample.jsonl")
    snapshot = read_jsonl(root / provenance["fixtures"]["tmdb-snapshots"]["source_path"])
    if tmdb_sample != snapshot[:len(tmdb_sample)]:
        raise ValueError("TMDB fixture does not match the real snapshot")
    sample_rows = read_reference(sample_dir / "movie-reference.sample.tsv")
    validate_reference_rows(sample_rows)
    sample_by_id = {int(row["movieId"]): reference_record(row) for row in sample_rows}
    if any(actual_by_id.get(movie_id) != row for movie_id, row in sample_by_id.items()):
        raise ValueError("Reference samples do not match canonical rows")
    if any(event["movieId"] not in sample_by_id for event in events):
        raise ValueError("Movie event samples cannot join with sample reference")
    passed("fixtures_are_real_and_joinable", movie_events=len(events), tmdb_snapshots=len(tmdb_sample), reference=len(sample_rows))

    api_key = os.getenv("TMDB_API_KEY", "").strip()
    if api_key and any(api_key in path.read_text(encoding="utf-8") for path in (root / "contracts").rglob("*") if path.is_file()):
        raise ValueError("A contract artifact contains the configured TMDB secret")
    passed("contract_artifacts_do_not_contain_configured_secret")

    tracked = subprocess.check_output(["git", "ls-files", "-z"], cwd=root).decode().split("\0")
    forbidden = [path for path in tracked if path == ".env" or path.startswith((".venv/", "data/raw/", "data/prepared/", "data/quarantine/"))]
    if forbidden:
        raise ValueError(f"Secret/dataset paths are tracked: {forbidden}")
    ignored = subprocess.run(["git", "check-ignore", ".env", ".venv/", "data/raw/", "data/prepared/", "data/quarantine/"], cwd=root, capture_output=True, text=True, check=True).stdout.splitlines()
    if len(ignored) != 5:
        raise ValueError("Missing ignore rules for secrets or datasets")
    passed("git_secret_and_dataset_exclusions", tracked_file_count=sum(bool(path) for path in tracked))

    if not (root / "DATA_CONTRACT.md").is_file():
        raise FileNotFoundError("DATA_CONTRACT.md is required at the project root")
    passed("data_contract_document_present")

    return {
        "generated_at": utc_now(), "status": "PASS", "scope": "Module 01 local data preparation through Step 15",
        "checks": checks,
        "warnings": ([{
            "rule": "tmdb_reference_coverage", "matched": tmdb_matched_count,
            "total": tmdb_record_count,
            "message": "Trending TMDB movies can be absent from the MovieLens base reference. Consumers must handle unmatched tmdbId without dropping events.",
        }] if tmdb_matched_count < tmdb_record_count else []),
        "pending": [
            "DATA_CONTRACT.md remains v1.0-draft; team approval to freeze is not recorded.",
            "Step 16: HDFS publish awaits an endpoint and infrastructure.",
            "Step 17: Kafka, MapReduce and Spark consumer confirmations are not available; local sample join passes.",
        ],
    }


def main() -> None:
    try:
        report = audit_module()
    except Exception as exc:
        report = {"generated_at": utc_now(), "status": "FAIL", "error": str(exc)}
        atomic_write_text(PROJECT_ROOT / "data/reports/module_01_audit.json", json.dumps(report, indent=2, ensure_ascii=False) + "\n")
        raise
    atomic_write_text(PROJECT_ROOT / "data/reports/module_01_audit.json", json.dumps(report, indent=2, ensure_ascii=False) + "\n")
    print("[OK] Offline project audit passed. HDFS and consumer handoff remain pending.")


if __name__ == "__main__":
    main()
