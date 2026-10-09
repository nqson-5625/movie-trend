from __future__ import annotations

import csv
import hashlib
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from modules.data_source.src.normalize_ids import (
    normalize_imdb_id,
    normalize_movie_id,
    normalize_tmdb_id,
)

# ============================================================
# CONFIG
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[3]
load_dotenv(PROJECT_ROOT / ".env")

MOVIELENS_VARIANT = os.getenv("MOVIELENS_VARIANT", "ml-latest-small")

MOVIELENS_DIR = PROJECT_ROOT / "data" / "raw" / "movielens" / MOVIELENS_VARIANT
IMDB_DIR = PROJECT_ROOT / "data" / "raw" / "imdb"
REPORT_DIR = PROJECT_ROOT / "data" / "reports"
PREPARED_REFERENCE_DIR = PROJECT_ROOT / "data" / "prepared" / "reference"

MOVIES_PATH = MOVIELENS_DIR / "movies.csv"
LINKS_PATH = MOVIELENS_DIR / "links.csv"
IMDB_BASICS_PATH = IMDB_DIR / "title.basics.tsv.gz"
IMDB_RATINGS_PATH = IMDB_DIR / "title.ratings.tsv.gz"
SOURCE_MANIFEST_PATH = REPORT_DIR / "source_manifest.json"
REFERENCE_OUTPUT_PATH = PREPARED_REFERENCE_DIR / "movie_reference.tsv"

IMDB_CHUNK_SIZE = 200_000

CANONICAL_COLUMNS = [
    "movieId",
    "title",
    "release_year",
    "genres",
    "imdbId",
    "tmdbId",
    "imdb_rating",
    "imdb_votes",
    "ref_version",
    "generated_at",
]

MOVIELENS_YEAR_PATTERN = re.compile(r"\((\d{4})\)\s*$")


# ============================================================
# COMMON HELPERS
# ============================================================

def utc_now() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(1024 * 1024)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def ensure_required_files() -> None:
    required_paths = [
        MOVIES_PATH,
        LINKS_PATH,
        IMDB_BASICS_PATH,
        IMDB_RATINGS_PATH,
        SOURCE_MANIFEST_PATH,
    ]
    missing = [str(p) for p in required_paths if not p.is_file()]
    if missing:
        raise RuntimeError(
            "Missing required input files:\n"
            + "\n".join(f"  - {p}" for p in missing)
        )


# ============================================================
# STEP 7 — LOAD + JOIN SOURCES
# ============================================================

def load_movies() -> pd.DataFrame:
    df = pd.read_csv(MOVIES_PATH, dtype=str)

    required = {"movieId", "title", "genres"}
    missing = required - set(df.columns)
    if missing:
        raise RuntimeError(f"movies.csv missing columns: {sorted(missing)}")

    df["movieId"] = df["movieId"].map(normalize_movie_id).astype("int64")

    if df["movieId"].duplicated().any():
        dup = (
            df.loc[df["movieId"].duplicated(keep=False), "movieId"]
            .tolist()
        )
        raise RuntimeError(f"Duplicate movieId in movies.csv: {dup[:20]}")

    return df[["movieId", "title", "genres"]].copy()


def load_links() -> pd.DataFrame:
    df = pd.read_csv(LINKS_PATH, dtype=str)

    required = {"movieId", "imdbId", "tmdbId"}
    missing = required - set(df.columns)
    if missing:
        raise RuntimeError(f"links.csv missing columns: {sorted(missing)}")

    df["movieId"] = df["movieId"].map(normalize_movie_id).astype("int64")
    df["imdbId"] = df["imdbId"].map(normalize_imdb_id).astype("string")
    df["tmdbId"] = df["tmdbId"].map(normalize_tmdb_id).astype("Int64")

    if df["movieId"].duplicated().any():
        dup = (
            df.loc[df["movieId"].duplicated(keep=False), "movieId"]
            .tolist()
        )
        raise RuntimeError(f"Duplicate movieId in links.csv: {dup[:20]}")

    return df[["movieId", "imdbId", "tmdbId"]].copy()


def join_movies_links(
    movies: pd.DataFrame,
    links: pd.DataFrame,
) -> pd.DataFrame:
    before = len(movies)

    result = movies.merge(
        links,
        how="left",
        on="movieId",
        validate="one_to_one",
    )

    if len(result) != before:
        raise RuntimeError(
            f"Row count changed after movies LEFT JOIN links: "
            f"{before} -> {len(result)}"
        )

    return result


def collect_target_imdb_ids(movie_links: pd.DataFrame) -> set[str]:
    return set(movie_links["imdbId"].dropna().astype(str))


def load_imdb_basics_subset(target_ids: set[str]) -> pd.DataFrame:
    output_columns = ["imdbId", "titleType", "primaryTitle", "startYear"]

    if not target_ids:
        return pd.DataFrame(columns=output_columns)

    read_columns = ["tconst", "titleType", "primaryTitle", "startYear"]
    matches: list[pd.DataFrame] = []

    total_scanned = 0
    total_matched = 0

    for chunk in pd.read_csv(
        IMDB_BASICS_PATH,
        sep="\t",
        compression="gzip",
        dtype=str,
        na_values="\\N",
        usecols=read_columns,
        chunksize=IMDB_CHUNK_SIZE,
        low_memory=False,
    ):
        total_scanned += len(chunk)

        hit = chunk[chunk["tconst"].isin(target_ids)]
        if not hit.empty:
            matches.append(hit.copy())
            total_matched += len(hit)

    print(f"[IMDb basics] scanned={total_scanned}, matched={total_matched}")

    if not matches:
        return pd.DataFrame(columns=output_columns)

    result = pd.concat(matches, ignore_index=True)

    if result["tconst"].duplicated().any():
        dup = (
            result.loc[result["tconst"].duplicated(keep=False), "tconst"]
            .tolist()
        )
        raise RuntimeError(
            f"Duplicate IMDb tconst in title.basics subset: {dup[:20]}"
        )

    result = result.rename(columns={"tconst": "imdbId"})
    return result[output_columns].copy()


def load_imdb_ratings_subset(target_ids: set[str]) -> pd.DataFrame:
    output_columns = ["imdbId", "averageRating", "numVotes"]

    if not target_ids:
        return pd.DataFrame(columns=output_columns)

    read_columns = ["tconst", "averageRating", "numVotes"]
    matches: list[pd.DataFrame] = []

    total_scanned = 0
    total_matched = 0

    for chunk in pd.read_csv(
        IMDB_RATINGS_PATH,
        sep="\t",
        compression="gzip",
        dtype=str,
        na_values="\\N",
        usecols=read_columns,
        chunksize=IMDB_CHUNK_SIZE,
        low_memory=False,
    ):
        total_scanned += len(chunk)

        hit = chunk[chunk["tconst"].isin(target_ids)]
        if not hit.empty:
            matches.append(hit.copy())
            total_matched += len(hit)

    print(f"[IMDb ratings] scanned={total_scanned}, matched={total_matched}")

    if not matches:
        return pd.DataFrame(columns=output_columns)

    result = pd.concat(matches, ignore_index=True)

    if result["tconst"].duplicated().any():
        dup = (
            result.loc[result["tconst"].duplicated(keep=False), "tconst"]
            .tolist()
        )
        raise RuntimeError(
            f"Duplicate IMDb tconst in title.ratings subset: {dup[:20]}"
        )

    result = result.rename(columns={"tconst": "imdbId"})
    return result[output_columns].copy()


def join_imdb_basics(
    movie_links: pd.DataFrame,
    imdb_basics: pd.DataFrame,
) -> pd.DataFrame:
    before = len(movie_links)

    result = movie_links.merge(
        imdb_basics,
        how="left",
        on="imdbId",
        validate="many_to_one",
    )

    if len(result) != before:
        raise RuntimeError(
            f"Row count changed after IMDb basics LEFT JOIN: "
            f"{before} -> {len(result)}"
        )

    return result


def join_imdb_ratings(
    movie_basics: pd.DataFrame,
    imdb_ratings: pd.DataFrame,
) -> pd.DataFrame:
    before = len(movie_basics)

    result = movie_basics.merge(
        imdb_ratings,
        how="left",
        on="imdbId",
        validate="many_to_one",
    )

    if len(result) != before:
        raise RuntimeError(
            f"Row count changed after IMDb ratings LEFT JOIN: "
            f"{before} -> {len(result)}"
        )

    return result


def build_joined_reference() -> pd.DataFrame:
    print("=" * 60)
    print("STEP 7 — Build Joined Reference")
    print("=" * 60)

    movies = load_movies()
    links = load_links()

    print(f"[MovieLens] movies rows: {len(movies)}")
    print(f"[MovieLens] links rows : {len(links)}")

    movie_links = join_movies_links(movies, links)
    print(f"[JOIN] movies + links : {len(movie_links)}")

    target_ids = collect_target_imdb_ids(movie_links)
    print(f"[TARGET] IMDb IDs     : {len(target_ids)}")

    imdb_basics = load_imdb_basics_subset(target_ids)
    imdb_ratings = load_imdb_ratings_subset(target_ids)

    joined = join_imdb_basics(movie_links, imdb_basics)
    joined = join_imdb_ratings(joined, imdb_ratings)

    if len(joined) != len(movies):
        raise RuntimeError(
            "Final Step-7 row count differs from MovieLens base table."
        )

    print(f"[OK] Step 7 final rows: {len(joined)}")
    return joined


def print_join_summary(df: pd.DataFrame) -> None:
    total = len(df)

    imdb_id_count = int(df["imdbId"].notna().sum())
    tmdb_id_count = int(df["tmdbId"].notna().sum())
    imdb_basics_match = int(df["startYear"].notna().sum())
    imdb_rating_match = int(df["averageRating"].notna().sum())

    def pct(value: int) -> float:
        return 0.0 if total == 0 else value / total * 100

    print()
    print("=" * 60)
    print("JOIN SUMMARY")
    print("=" * 60)
    print(f"MovieLens movies        : {total}")
    print(f"Has IMDb ID             : {imdb_id_count}")
    print(f"Has TMDB ID             : {tmdb_id_count}")
    print(f"Matched IMDb basics     : {imdb_basics_match}")
    print(f"Matched IMDb ratings    : {imdb_rating_match}")
    print(f"IMDb ID coverage        : {pct(imdb_id_count):.2f}%")
    print(f"TMDB ID coverage        : {pct(tmdb_id_count):.2f}%")
    print(f"IMDb basics match rate  : {pct(imdb_basics_match):.2f}%")
    print(f"IMDb rating match rate  : {pct(imdb_rating_match):.2f}%")


# ============================================================
# STEP 8 — BUILD CANONICAL FIELDS
# ============================================================

def parse_year_from_movielens_title(title) -> int | None:
    if title is None or pd.isna(title):
        return None

    value = str(title).strip()
    if not value:
        return None

    match = MOVIELENS_YEAR_PATTERN.search(value)
    if not match:
        return None

    return int(match.group(1))


def parse_imdb_start_year(value) -> int | None:
    if value is None or pd.isna(value):
        return None

    value = str(value).strip()
    if not value or not value.isdigit():
        return None

    return int(value)


def derive_release_year(
    imdb_start_year,
    movielens_title,
) -> int | None:
    imdb_year = parse_imdb_start_year(imdb_start_year)

    if imdb_year is not None:
        return imdb_year

    return parse_year_from_movielens_title(movielens_title)


def convert_imdb_rating(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype("Float64")


def convert_imdb_votes(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype("Int64")


def build_canonical_reference(
    joined: pd.DataFrame,
    ref_version: str,
    generated_at: str,
) -> pd.DataFrame:
    required_input = {
        "movieId",
        "title",
        "genres",
        "imdbId",
        "tmdbId",
        "startYear",
        "averageRating",
        "numVotes",
    }

    missing = required_input - set(joined.columns)
    if missing:
        raise RuntimeError(
            f"Joined reference is missing columns: {sorted(missing)}"
        )

    canonical = pd.DataFrame(index=joined.index)

    canonical["movieId"] = joined["movieId"].astype("int64")
    canonical["title"] = joined["title"].copy()

    release_year = [
        derive_release_year(start_year, title)
        for start_year, title in zip(
            joined["startYear"],
            joined["title"],
        )
    ]

    canonical["release_year"] = pd.Series(
        release_year,
        index=joined.index,
        dtype="Int64",
    )

    canonical["genres"] = joined["genres"].copy()
    canonical["imdbId"] = joined["imdbId"].astype("string")
    canonical["tmdbId"] = joined["tmdbId"].astype("Int64")

    canonical["imdb_rating"] = convert_imdb_rating(
        joined["averageRating"]
    )

    canonical["imdb_votes"] = convert_imdb_votes(
        joined["numVotes"]
    )

    canonical["ref_version"] = ref_version
    canonical["generated_at"] = generated_at

    canonical = canonical[CANONICAL_COLUMNS]

    if len(canonical) != len(joined):
        raise RuntimeError(
            f"Canonical row count changed: {len(joined)} -> {len(canonical)}"
        )

    return canonical


# ============================================================
# STEP 9 — VERSION FROM SOURCE MANIFEST
# ============================================================

def load_source_manifest() -> dict:
    if not SOURCE_MANIFEST_PATH.is_file():
        raise RuntimeError(
            f"source_manifest.json not found: {SOURCE_MANIFEST_PATH}"
        )

    with SOURCE_MANIFEST_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


def get_manifest_file_sha256(
    manifest: dict,
    source_name: str,
    filename: str,
) -> str:
    for source in manifest.get("sources", []):
        if source.get("source_name") != source_name:
            continue

        for file_info in source.get("files", []):
            if file_info.get("name") != filename:
                continue

            checksum = file_info.get("sha256")

            if not checksum:
                raise RuntimeError(
                    f"Missing SHA256 for {source_name}/{filename}"
                )

            return checksum

    raise RuntimeError(
        f"Checksum not found in manifest: {source_name}/{filename}"
    )


def calculate_reference_source_hash(manifest: dict) -> str:
    """
    Only sources that actually influence movie_reference are included.
    ratings.csv is intentionally excluded.
    """

    source_checksums = [
        (
            "MovieLens",
            "movies.csv",
            get_manifest_file_sha256(
                manifest,
                "MovieLens",
                "movies.csv",
            ),
        ),
        (
            "MovieLens",
            "links.csv",
            get_manifest_file_sha256(
                manifest,
                "MovieLens",
                "links.csv",
            ),
        ),
        (
            "IMDb",
            "title.basics.tsv.gz",
            get_manifest_file_sha256(
                manifest,
                "IMDb",
                "title.basics.tsv.gz",
            ),
        ),
        (
            "IMDb",
            "title.ratings.tsv.gz",
            get_manifest_file_sha256(
                manifest,
                "IMDb",
                "title.ratings.tsv.gz",
            ),
        ),
    ]

    source_checksums.sort(key=lambda item: (item[0], item[1]))

    canonical_input = "\n".join(
        f"{source}|{filename}|{checksum}"
        for source, filename, checksum in source_checksums
    )

    return hashlib.sha256(
        canonical_input.encode("utf-8")
    ).hexdigest()


def build_ref_version(
    source_hash: str,
    generated_at: str,
) -> str:
    date_part = generated_at[0:10].replace("-", "")
    hash_prefix = source_hash[:8]
    return f"{date_part}-{hash_prefix}"


# ============================================================
# STEP 9 — WRITE FINAL TSV
# ============================================================

def write_reference_atomic(
    df: pd.DataFrame,
    output_path: Path,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    temp_path = output_path.parent / f"{output_path.name}.tmp"

    try:
        df.to_csv(
            temp_path,
            sep="\t",
            index=False,
            encoding="utf-8",
            na_rep="",
            quoting=csv.QUOTE_MINIMAL,
            lineterminator="\n",
        )

        os.replace(temp_path, output_path)

    finally:
        if temp_path.exists():
            temp_path.unlink()


def build_and_write_reference() -> dict:
    """

    B1: load + normalize + LEFT JOIN sources
    B2: derive exact canonical fields
    B3: calculate ref_version + atomic-write TSV

    """

    ensure_required_files()

    generated_at = utc_now()

    manifest = load_source_manifest()
    source_hash = calculate_reference_source_hash(manifest)
    ref_version = build_ref_version(
        source_hash=source_hash,
        generated_at=generated_at,
    )

    print("=" * 60)
    print("MOVIE REFERENCE BUILD")
    print("=" * 60)
    print(f"generated_at : {generated_at}")
    print(f"source_hash  : {source_hash}")
    print(f"ref_version  : {ref_version}")
    print()

    joined = build_joined_reference()
    print_join_summary(joined)

    canonical = build_canonical_reference(
        joined=joined,
        ref_version=ref_version,
        generated_at=generated_at,
    )

    print()
    print("=" * 60)
    print("STEP 8 — CANONICAL SCHEMA")
    print("=" * 60)
    print("Columns:", canonical.columns.tolist())
    print("Rows   :", len(canonical))
    print()
    print(canonical.head(10).to_string(index=False))

    write_reference_atomic(
        canonical,
        REFERENCE_OUTPUT_PATH,
    )

    output_sha256 = sha256_file(
        REFERENCE_OUTPUT_PATH
    )

    result = {
        "path": str(REFERENCE_OUTPUT_PATH),
        "row_count": int(len(canonical)),
        "ref_version": ref_version,
        "generated_at": generated_at,
        "source_hash": source_hash,
        "sha256": output_sha256,
    }

    print()
    print("=" * 60)
    print("STEP 9 — OUTPUT")
    print("=" * 60)
    print(f"Path        : {result['path']}")
    print(f"Rows        : {result['row_count']}")
    print(f"ref_version : {result['ref_version']}")
    print(f"generated_at: {result['generated_at']}")
    print(f"SHA256      : {result['sha256']}")
    print()
    print("[OK] Steps 7-9 completed.")
    print("Next: Step 10 validate movie_reference.tsv.")

    return result


# ============================================================
# MAIN
# ============================================================

def main() -> None:
    result = build_and_write_reference()

    print()
    print(
        json.dumps(
            result,
            indent=2,
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
