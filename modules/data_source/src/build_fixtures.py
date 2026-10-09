"""Step 14: cut small, schema-valid fixtures from real local artifacts."""
from __future__ import annotations

import argparse
import csv
import io
import json
from itertools import islice
from pathlib import Path

from modules.data_source.src import build_movie_reference as reference
from modules.data_source.src.contract_validation import (
    PROJECT_ROOT, REFERENCE_FIELDS, atomic_write_text, jsonl_text, load_validator,
    read_jsonl, read_reference, validate_record, validate_reference_rows,
)
from modules.data_source.src.movie_event_adapter import to_movie_event
from modules.data_source.src.tmdb_source_adapter import utc_now


def build_fixtures(
    project_root: Path = PROJECT_ROOT,
    count: int = 20,
    snapshot_path: Path | None = None,
    ingest_time: str | None = None,
) -> dict:
    if not 20 <= count <= 100:
        raise ValueError("Fixture count must be between 20 and 100")
    ratings_path = project_root / "data/prepared/movielens/ratings_sorted.csv"
    reference_path = project_root / "data/prepared/reference/movie_reference.tsv"
    if snapshot_path is None:
        snapshots = sorted((project_root / "data/prepared/tmdb").glob("snapshot_*.jsonl"))
        if not snapshots:
            raise FileNotFoundError("No real TMDB snapshot. Run tmdb_source_adapter.py --once first.")
        snapshot_path = snapshots[-1]
    snapshot_path = snapshot_path.resolve()
    snapshot_relative_path = str(snapshot_path.relative_to(project_root.resolve()))
    # Preserve the actual snapshot's timestamps and event IDs.
    tmdb_records = read_jsonl(snapshot_path)[:count]
    with ratings_path.open(encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream)
        if reader.fieldnames != ["userId", "movieId", "rating", "timestamp"]:
            raise ValueError("Unexpected ratings_sorted.csv header")
        rating_rows = list(islice(reader, count))
    fixture_time = ingest_time or utc_now()
    movie_records = [to_movie_event(row, ingest_time=fixture_time) for row in rating_rows]

    reference_rows = read_reference(reference_path)
    validate_reference_rows(reference_rows, project_root)
    # Include every sampled rating's movieId so consumers can smoke-test a join.
    by_id = {int(row["movieId"]): row for row in reference_rows}
    selected = {}
    for event in movie_records:
        movie_id = event["movieId"]
        if movie_id not in by_id:
            raise ValueError(f"Sample movieId missing from reference: {movie_id}")
        selected[movie_id] = by_id[movie_id]
    # Where space permits, include real missing mappings and the genre sentinel.
    for predicate in (
        lambda row: row["tmdbId"] == "",
        lambda row: row["imdb_rating"] == "",
        lambda row: row["genres"] == "(no genres listed)",
    ):
        if len(selected) < count:
            candidate = next((row for row in reference_rows if predicate(row)), None)
            if candidate is not None:
                selected[int(candidate["movieId"])] = candidate
    for row in reference_rows:
        if len(selected) >= count:
            break
        selected[int(row["movieId"])] = row
    sample_reference = list(selected.values())

    groups = {"movie-events": movie_records, "tmdb-snapshots": tmdb_records, "movie-reference": sample_reference}
    for name, records in groups.items():
        if len(records) != count:
            raise ValueError(f"{name} has only {len(records)} real rows; requested {count}")
        if name != "movie-reference":
            validator = load_validator(name, project_root)
            for record in records:
                validate_record(record, validator)
            if len({record["event_id"] for record in records}) != len(records):
                raise ValueError(f"Duplicate event_id in {name} fixture")
    validate_reference_rows(sample_reference, project_root)

    tsv = io.StringIO(newline="")
    writer = csv.DictWriter(tsv, fieldnames=REFERENCE_FIELDS, delimiter="\t", lineterminator="\n")
    writer.writeheader()
    writer.writerows(sample_reference)
    sample_dir = project_root / "contracts/samples"
    # Prepare and validate all three before publishing any fixture.
    for name, content in (
        ("movie-events.sample.jsonl", jsonl_text(movie_records)),
        ("tmdb-snapshots.sample.jsonl", jsonl_text(tmdb_records)),
        ("movie-reference.sample.tsv", tsv.getvalue()),
    ):
        atomic_write_text(sample_dir / name, content)

    provenance = {
        "generated_at": fixture_time,
        "note": "Movie event ingest_time is fixture creation time, not evidence of Kafka arrival.",
        "fixtures": {
            "movie-events": {"source_path": str(ratings_path.relative_to(project_root)), "source_sha256": reference.sha256_file(ratings_path), "row_count": count},
            "tmdb-snapshots": {"source_path": snapshot_relative_path, "source_sha256": reference.sha256_file(snapshot_path), "row_count": count},
            "movie-reference": {"source_path": str(reference_path.relative_to(project_root)), "source_sha256": reference.sha256_file(reference_path), "row_count": count},
        },
    }
    atomic_write_text(sample_dir / "fixture_provenance.json", json.dumps(provenance, ensure_ascii=False, indent=2) + "\n")
    return provenance


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=20)
    parser.add_argument("--snapshot", type=Path, help="A real snapshot inside this project")
    parser.add_argument("--ingest-time", help="Explicit UTC fixture time for reproducible MovieLens samples")
    args = parser.parse_args()
    provenance = build_fixtures(count=args.count, snapshot_path=args.snapshot, ingest_time=args.ingest_time)
    print("[OK] Three real fixtures validated:", {name: item["row_count"] for name, item in provenance["fixtures"].items()})


if __name__ == "__main__":
    main()
