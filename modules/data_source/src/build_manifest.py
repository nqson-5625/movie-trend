"""Step 15: verify raw provenance and describe the prepared Module 01 build."""
from __future__ import annotations

import argparse
import copy
import csv
import json
from pathlib import Path

from modules.data_source.src import build_movie_reference as reference
from modules.data_source.src.contract_validation import (
    PROJECT_ROOT, atomic_write_text, load_validator, read_jsonl, read_reference,
    validate_record, validate_reference_rows,
)
from modules.data_source.src.tmdb_source_adapter import (
    TMDB_BASE_URL, TMDB_TRENDING_ENDPOINT, build_tmdb_event_id,
    normalize_utc_iso, timestamp_for_filename, utc_now,
)


def file_metadata(path: Path, project_root: Path) -> dict:
    return {
        "filename": path.name,
        "path": str(path.relative_to(project_root)),
        "size_bytes": path.stat().st_size,
        "sha256": reference.sha256_file(path),
    }


def verify_raw_sources(manifest: dict, project_root: Path, variant: str) -> list[dict]:
    sources = copy.deepcopy(manifest["sources"])
    for name, directory, required in (
        ("MovieLens", project_root / "data/raw/movielens" / variant, {"movies.csv", "ratings.csv", "links.csv"}),
        ("IMDb", project_root / "data/raw/imdb", {"title.basics.tsv.gz", "title.ratings.tsv.gz"}),
    ):
        matches = [source for source in sources if source["source_name"] == name]
        if len(matches) != 1:
            raise ValueError(f"Expected one {name} provenance entry for this build")
        source = matches[0]
        if name == "IMDb":
            source.setdefault("source_url", "https://datasets.imdbws.com/")
        if name == "MovieLens" and source.get("variant") != variant:
            raise ValueError("MovieLens provenance variant does not match this build")
        files = source["files"]
        filenames = [item.get("name", item.get("filename")) for item in files]
        if len(set(filenames)) != len(filenames) or not required.issubset(filenames):
            raise ValueError(f"Missing or duplicate required {name} provenance files")
        for item, filename in zip(files, filenames):
            if not filename or Path(filename).name != filename:
                raise ValueError("Provenance filename must be a plain filename")
            path = directory / filename
            actual = file_metadata(path, project_root)
            if actual["sha256"] != item.get("sha256") or actual["size_bytes"] != item.get("size_bytes"):
                raise ValueError(f"Raw checksum/size mismatch: {actual['path']}; do not refresh provenance to hide a changed source")
            item.update(actual)
            item["name"] = filename  # Keep compatibility with the existing reference builder.
            item["source_name"] = name
            item["source_url"] = item.get("source_url") or source.get("source_url")
            if not item["source_url"]:
                raise ValueError(f"Missing original source URL: {name}/{filename}")
            item["retrieved_at"] = normalize_utc_iso(item.get("retrieved_at") or source.get("retrieved_at"))
    return sources


def snapshot_metadata(path: Path, project_root: Path) -> dict:
    records = read_jsonl(path)
    validator = load_validator("tmdb-snapshots", project_root)
    for record in records:
        validate_record(record, validator)
        if record["event_id"] != build_tmdb_event_id(record["tmdbId"], record["event_time"]):
            raise ValueError(f"TMDB event_id mismatch in {path.name}")
    times = {record["event_time"] for record in records}
    if len(times) != 1 or len({record["tmdbId"] for record in records}) != len(records):
        raise ValueError(f"Invalid snapshot time or duplicate TMDB ID in {path.name}")
    return {
        **file_metadata(path, project_root),
        "name": path.name,
        "source_name": "TMDB",
        # Canonical endpoint, without credentials; this is not a raw HTTP dump.
        "source_url": TMDB_BASE_URL + TMDB_TRENDING_ENDPOINT,
        "retrieved_at": records[0]["event_time"],
        "row_count": len(records),
        "artifact_kind": "normalized_snapshot",
    }


def build_manifest(project_root: Path = PROJECT_ROOT, variant: str | None = None) -> dict:
    variant = variant or reference.MOVIELENS_VARIANT
    manifest_path = project_root / "data/reports/source_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    sources = verify_raw_sources(manifest, project_root, variant)
    source_hash = reference.calculate_reference_source_hash({"sources": sources})
    reference_path = project_root / "data/prepared/reference/movie_reference.tsv"
    rows = read_reference(reference_path)
    validate_reference_rows(rows, project_root)
    generated_at, ref_version = rows[0]["generated_at"], rows[0]["ref_version"]
    if ref_version != reference.build_ref_version(source_hash, generated_at):
        raise ValueError("Reference version does not match the verified source checksums; rebuild reference first")
    with (project_root / "data/raw/movielens" / variant / "movies.csv").open(encoding="utf-8", newline="") as stream:
        movie_ids = {int(row["movieId"]) for row in csv.DictReader(stream)}
    if {int(row["movieId"]) for row in rows} != movie_ids:
        raise ValueError("Reference movie IDs differ from the MovieLens base table")

    snapshots = sorted((project_root / "data/prepared/tmdb").glob("snapshot_*.jsonl"))
    if not snapshots:
        raise FileNotFoundError("No TMDB snapshot; run the TMDB adapter first")
    tmdb_files = [snapshot_metadata(path, project_root) for path in snapshots]
    sources = [source for source in sources if source.get("source_name") != "TMDB"]
    sources.append({
        "source_name": "TMDB",
        "source_url": TMDB_BASE_URL + TMDB_TRENDING_ENDPOINT,
        "retrieved_at": max(item["retrieved_at"] for item in tmdb_files),
        "files": tmdb_files,
    })

    sorted_path = project_root / "data/prepared/movielens/ratings_sorted.csv"
    with sorted_path.open(encoding="utf-8", newline="") as stream:
        ratings = list(csv.DictReader(stream))
    fixtures = []
    for name, suffix in (("movie-events", "jsonl"), ("tmdb-snapshots", "jsonl"), ("movie-reference", "tsv")):
        path = project_root / "contracts/samples" / f"{name}.sample.{suffix}"
        if suffix == "tsv":
            records = read_reference(path)
            validate_reference_rows(records, project_root)
        else:
            records = read_jsonl(path)
            validator = load_validator(name, project_root)
            for record in records:
                validate_record(record, validator)
        if not 20 <= len(records) <= 100:
            raise ValueError(f"{name} fixture must have 20–100 real records")
        fixtures.append({**file_metadata(path, project_root), "row_count": len(records), "schema": f"contracts/schemas/{name}.schema.json"})

    manifest.update({
        "build_id": timestamp_for_filename(utc_now()),
        "movielens_variant": variant,
        "sources": sources,
        "reference": {
            **file_metadata(reference_path, project_root), "row_count": len(rows),
            "ref_version": ref_version, "generated_at": generated_at, "source_hash": source_hash,
        },
        "prepared": [{**file_metadata(sorted_path, project_root), "row_count": len(ratings)}],
        "fixtures": fixtures,
    })
    atomic_write_text(manifest_path, json.dumps(manifest, ensure_ascii=False, indent=2) + "\n")
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    manifest = build_manifest()
    print(f"[OK] Manifest {manifest['build_id']}: reference={manifest['reference']['row_count']} rows, ref_version={manifest['reference']['ref_version']}")


if __name__ == "__main__":
    main()
