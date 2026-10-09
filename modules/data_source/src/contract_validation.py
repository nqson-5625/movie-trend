"""Local contract validation shared by fixtures, manifest and project audit."""
from __future__ import annotations

import csv
import json
import math
import os
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Iterable

from jsonschema import Draft202012Validator, FormatChecker

PROJECT_ROOT = Path(__file__).resolve().parents[3]
REFERENCE_FIELDS = [
    "movieId", "title", "release_year", "genres", "imdbId", "tmdbId",
    "imdb_rating", "imdb_votes", "ref_version", "generated_at",
]
NULLABLE_FIELDS = {"release_year", "imdbId", "tmdbId", "imdb_rating", "imdb_votes"}
INTEGER_FIELDS = {"movieId", "release_year", "tmdbId", "imdb_votes"}


def load_validator(name: str, project_root: Path = PROJECT_ROOT) -> Draft202012Validator:
    path = project_root / "contracts" / "schemas" / f"{name}.schema.json"
    schema = json.loads(path.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    checker = FormatChecker()

    @checker.checks("date-time", raises=ValueError)
    def valid_utc_timestamp(value):
        # jsonschema's default date-time checker requires an optional package.
        # Our contracts accept only UTC timestamps with whole seconds.
        if not isinstance(value, str):
            return True  # The schema's type constraint handles this case.
        datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
        return True

    return Draft202012Validator(schema, format_checker=checker)


def validate_record(record: dict, validator: Draft202012Validator) -> None:
    # Python's JSON parser and jsonschema can otherwise accept NaN/Infinity.
    if any(isinstance(value, float) and not math.isfinite(value) for value in record.values()):
        raise ValueError("Contract numbers must be finite")
    validator.validate(record)


def reject_json_constant(value: str):
    raise ValueError(f"Invalid JSON numeric constant: {value}")


def read_jsonl(path: Path) -> list[dict]:
    records = []
    with path.open(encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, 1):
            try:
                record = json.loads(line, parse_constant=reject_json_constant)
            except ValueError as exc:
                raise ValueError(f"{path.name}, line {line_number}: invalid JSON") from exc
            if not isinstance(record, dict):
                raise ValueError(f"{path.name}, line {line_number}: expected an object")
            records.append(record)
    if not records:
        raise ValueError(f"Empty JSONL artifact: {path}")
    return records


def reference_record(row: dict[str, str]) -> dict:
    """Decode the canonical TSV without converting missing IDs to zero."""
    if list(row) != REFERENCE_FIELDS or any(value is None for value in row.values()):
        raise ValueError("Reference TSV must have exactly the ten canonical columns")
    record = {}
    for field, value in row.items():
        if field in NULLABLE_FIELDS and value == "":
            record[field] = None
        elif field in INTEGER_FIELDS:
            record[field] = int(value)
        elif field == "imdb_rating":
            record[field] = float(value)
        else:
            record[field] = value
    return record


def read_reference(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream, delimiter="\t")
        if reader.fieldnames != REFERENCE_FIELDS:
            raise ValueError("Reference TSV header differs from the contract")
        rows = list(reader)
    if not rows:
        raise ValueError(f"Empty reference: {path}")
    return rows


def validate_reference_rows(rows: list[dict[str, str]], project_root: Path = PROJECT_ROOT) -> None:
    validator = load_validator("movie-reference", project_root)
    seen = set()
    for row in rows:
        record = reference_record(row)
        validate_record(record, validator)
        if record["movieId"] in seen:
            raise ValueError(f"Duplicate reference movieId: {record['movieId']}")
        seen.add(record["movieId"])
    if len({row["ref_version"] for row in rows}) != 1 or len({row["generated_at"] for row in rows}) != 1:
        raise ValueError("Reference must contain one ref_version and one generated_at")


def atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", newline="", dir=path.parent,
            prefix=path.name + ".", suffix=".tmp", delete=False,
        ) as stream:
            temporary = Path(stream.name)
            stream.write(content)
        os.replace(temporary, path)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()


def jsonl_text(records: Iterable[dict]) -> str:
    return "".join(json.dumps(record, ensure_ascii=False, allow_nan=False, separators=(",", ":")) + "\n" for record in records)
