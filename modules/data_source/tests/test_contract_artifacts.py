"""Contract, real fixture extraction and provenance failure-path tests."""
import csv
import gzip
import json
import re
import shutil
from datetime import datetime

import pytest
from jsonschema import ValidationError

from modules.data_source.src import build_movie_reference as reference
from modules.data_source.src.build_fixtures import build_fixtures
from modules.data_source.src.build_manifest import build_manifest
from modules.data_source.src.contract_validation import (
    PROJECT_ROOT, REFERENCE_FIELDS, load_validator, read_jsonl,
    read_reference, reference_record, validate_record, validate_reference_rows,
)
from modules.data_source.src.movie_event_adapter import to_movie_event
from modules.data_source.src.tmdb_source_adapter import normalize_tmdb_item


@pytest.mark.parametrize("name", ["movie-events", "tmdb-snapshots", "movie-reference"])
def test_committed_real_fixtures_validate_against_schemas(name):
    directory = PROJECT_ROOT / "contracts/samples"
    if name == "movie-reference":
        rows = read_reference(directory / f"{name}.sample.tsv")
        validate_reference_rows(rows)
    else:
        rows = read_jsonl(directory / f"{name}.sample.jsonl")
        validator = load_validator(name)
        for record in rows:
            validate_record(record, validator)
    assert 20 <= len(rows) <= 100


@pytest.mark.parametrize("field,value", [
    ("rating", 0), ("userId", True), ("movieId", 0),
    ("event_time", "2026-02-30T00:00:00Z"),
    ("event_time", "2026-10-08T00:00:00+07:00"),
    ("event_id", "not-a-hash"), ("extra", "secret"),
])
def test_event_schema_rejects_invalid_contract_values(field, value):
    event = read_jsonl(PROJECT_ROOT / "contracts/samples/movie-events.sample.jsonl")[0]
    event[field] = value
    with pytest.raises(ValidationError):
        validate_record(event, load_validator("movie-events"))


@pytest.mark.parametrize("value", ["nan", "inf", "-inf"])
def test_adapters_reject_non_finite_numbers(value):
    with pytest.raises(ValueError):
        to_movie_event({"userId": 1, "movieId": 1, "rating": value, "timestamp": 1}, "2026-10-08T00:00:00Z")
    item = {"id": 1, "title": "Movie", "popularity": value, "vote_count": 1, "vote_average": 1}
    with pytest.raises(ValueError):
        normalize_tmdb_item(item, "2026-10-08T00:00:00Z", "2026-10-08T00:00:00Z")


@pytest.mark.parametrize("value", [1.5, True, "2.8"])
def test_tmdb_vote_count_is_not_silently_truncated(value):
    item = {"id": 1, "title": "Movie", "popularity": 1, "vote_count": value, "vote_average": 1}
    with pytest.raises(ValueError):
        normalize_tmdb_item(item, "2026-10-08T00:00:00Z", "2026-10-08T00:00:00Z")


def test_reference_null_and_genre_serialization():
    row = read_reference(PROJECT_ROOT / "contracts/samples/movie-reference.sample.tsv")[0]
    for field in ("imdbId", "tmdbId", "release_year", "imdb_rating", "imdb_votes"):
        row[field] = ""
    row["genres"] = "(no genres listed)"
    record = reference_record(row)
    validate_record(record, load_validator("movie-reference"))
    assert record["tmdbId"] is None
    assert record["imdbId"] is None
    assert record["genres"] == "(no genres listed)"
    row["tmdbId"] = "1.5"
    with pytest.raises(ValueError):
        reference_record(row)


def test_tmdb_schema_enforces_non_negative_popularity():
    record = read_jsonl(PROJECT_ROOT / "contracts/samples/tmdb-snapshots.sample.jsonl")[0]
    validator = load_validator("tmdb-snapshots")
    record["popularity"] = 0
    validate_record(record, validator)
    record["popularity"] = -0.01
    with pytest.raises(ValidationError):
        validate_record(record, validator)


def test_reference_duplicate_id_is_rejected():
    row = read_reference(PROJECT_ROOT / "contracts/samples/movie-reference.sample.tsv")[0]
    with pytest.raises(ValueError, match="Duplicate"):
        validate_reference_rows([row, row])


def test_jsonl_rejects_nan(tmp_path):
    path = tmp_path / "invalid.jsonl"
    path.write_text('{"rating":NaN}\n')
    with pytest.raises(ValueError, match="invalid JSON"):
        read_jsonl(path)


@pytest.fixture
def small_project(tmp_path):
    """Small deterministic input project; production evidence uses actual datasets."""
    shutil.copytree(PROJECT_ROOT / "contracts/schemas", tmp_path / "contracts/schemas")
    rows = read_reference(PROJECT_ROOT / "contracts/samples/movie-reference.sample.tsv")
    events = read_jsonl(PROJECT_ROOT / "contracts/samples/movie-events.sample.jsonl")
    raw = tmp_path / "data/raw/movielens/ml-latest-small"
    raw.mkdir(parents=True)

    def write_csv(path, fieldnames, records, delimiter=","):
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=fieldnames, delimiter=delimiter, lineterminator="\n")
            writer.writeheader()
            writer.writerows(records)

    write_csv(raw / "movies.csv", ["movieId", "title", "genres"], [{field: row[field] for field in ("movieId", "title", "genres")} for row in rows])
    write_csv(raw / "links.csv", ["movieId", "imdbId", "tmdbId"], [{field: row[field] for field in ("movieId", "imdbId", "tmdbId")} for row in rows])
    ratings = [{"userId": event["userId"], "movieId": event["movieId"], "rating": event["rating"], "timestamp": int(datetime.fromisoformat(event["event_time"].replace("Z", "+00:00")).timestamp())} for event in events]
    write_csv(raw / "ratings.csv", ["userId", "movieId", "rating", "timestamp"], ratings)
    sorted_path = tmp_path / "data/prepared/movielens/ratings_sorted.csv"
    sorted_path.parent.mkdir(parents=True)
    shutil.copyfile(raw / "ratings.csv", sorted_path)
    imdb_dir = tmp_path / "data/raw/imdb"
    imdb_dir.mkdir(parents=True)
    for filename in ("title.basics.tsv.gz", "title.ratings.tsv.gz"):
        (imdb_dir / filename).write_bytes(gzip.compress(b"minimal unit test input\n", mtime=0))
    sources = []
    for name, directory, url in (("MovieLens", raw, "https://files.grouplens.org/datasets/movielens/ml-latest-small.zip"), ("IMDb", imdb_dir, "https://datasets.imdbws.com/")):
        sources.append({
            "source_name": name, "variant": "ml-latest-small", "source_url": url,
            "retrieved_at": "2026-10-04T00:00:00Z", "archive_sha256": "preserved-download-evidence",
            "files": [{"name": path.name, "sha256": reference.sha256_file(path), "size_bytes": path.stat().st_size} for path in sorted(directory.iterdir())],
        })
    manifest = {"build_id": "old-build", "movielens_variant": "ml-latest-small", "sources": sources}
    manifest_path = tmp_path / "data/reports/source_manifest.json"
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text(json.dumps(manifest))
    version = reference.build_ref_version(reference.calculate_reference_source_hash(manifest), rows[0]["generated_at"])
    for row in rows:
        row["ref_version"] = version
    write_csv(tmp_path / "data/prepared/reference/movie_reference.tsv", REFERENCE_FIELDS, rows, "\t")
    snapshot = tmp_path / "data/prepared/tmdb/snapshot_20261008T104404Z.jsonl"
    snapshot.parent.mkdir(parents=True)
    shutil.copyfile(PROJECT_ROOT / "contracts/samples/tmdb-snapshots.sample.jsonl", snapshot)
    return tmp_path


def test_fixture_extraction_is_reproducible_and_joinable(small_project):
    root = small_project
    provenance = build_fixtures(root, ingest_time="2026-10-08T00:00:00Z")
    sample = root / "contracts/samples/movie-events.sample.jsonl"
    first = sample.read_bytes()
    build_fixtures(root, ingest_time="2026-10-08T00:00:00Z")
    assert sample.read_bytes() == first
    events = read_jsonl(sample)
    movie_ids = {int(row["movieId"]) for row in read_reference(root / "contracts/samples/movie-reference.sample.tsv")}
    assert all(event["movieId"] in movie_ids for event in events)
    assert provenance["fixtures"]["movie-events"]["source_sha256"] == reference.sha256_file(root / "data/prepared/movielens/ratings_sorted.csv")


def test_fixture_failure_preserves_previous_samples(small_project):
    build_fixtures(small_project)
    sample = small_project / "contracts/samples/movie-events.sample.jsonl"
    before = sample.read_bytes()
    with pytest.raises(ValueError, match="real rows"):
        build_fixtures(small_project, count=21)
    assert sample.read_bytes() == before


def test_manifest_keeps_provenance_and_describes_verified_reference(small_project):
    build_fixtures(small_project)
    manifest = build_manifest(small_project, "ml-latest-small")
    assert re.fullmatch(r"[0-9]{8}T[0-9]{6}Z", manifest["build_id"])
    assert manifest["reference"]["row_count"] == 20
    assert manifest["reference"]["sha256"] == reference.sha256_file(small_project / manifest["reference"]["path"])
    assert len(manifest["fixtures"]) == 3
    assert manifest["sources"][0]["archive_sha256"] == "preserved-download-evidence"
    assert manifest["sources"][0]["retrieved_at"] == "2026-10-04T00:00:00Z"
    for source in manifest["sources"]:
        for item in source["files"]:
            assert all(field in item for field in ("source_name", "source_url", "retrieved_at", "filename", "size_bytes", "sha256"))


def test_manifest_refuses_to_hide_modified_raw_sources(small_project):
    build_fixtures(small_project)
    manifest_path = small_project / "data/reports/source_manifest.json"
    before = manifest_path.read_bytes()
    path = small_project / "data/raw/movielens/ml-latest-small/ratings.csv"
    path.write_text(path.read_text() + "1,1,1.0,1\n")
    with pytest.raises(ValueError, match="checksum/size mismatch"):
        build_manifest(small_project, "ml-latest-small")
    assert manifest_path.read_bytes() == before
