from __future__ import annotations

import json
import os
import sys

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv


# ============================================================
# PROJECT CONFIG
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[3]

load_dotenv(PROJECT_ROOT / ".env")

MOVIELENS_VARIANT = os.getenv(
    "MOVIELENS_VARIANT",
    "ml-latest-small",
)

MOVIELENS_DIR = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "movielens"
    / MOVIELENS_VARIANT
)

IMDB_DIR = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "imdb"
)

REPORT_DIR = (
    PROJECT_ROOT
    / "data"
    / "reports"
)

REPORT_PATH = (
    REPORT_DIR
    / "data_profile.json"
)

IMDB_CHUNK_SIZE = 200_000


# ============================================================
# REQUIRED COLUMNS
# ============================================================

RATINGS_COLUMNS = [
    "userId",
    "movieId",
    "rating",
    "timestamp",
]

MOVIES_COLUMNS = [
    "movieId",
    "title",
    "genres",
]

LINKS_COLUMNS = [
    "movieId",
    "imdbId",
    "tmdbId",
]

IMDB_BASICS_COLUMNS = [
    "tconst",
    "titleType",
    "primaryTitle",
    "startYear",
    "genres",
]

IMDB_RATINGS_COLUMNS = [
    "tconst",
    "averageRating",
    "numVotes",
]


# ============================================================
# HELPERS
# ============================================================

def utc_now() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def safe_number(value):
    """
    Convert pandas/numpy numeric values to JSON-safe values.
    """

    if value is None:
        return None

    if pd.isna(value):
        return None

    if isinstance(value, bool):
        return value

    try:
        if float(value).is_integer():
            return int(value)
    except (TypeError, ValueError):
        pass

    try:
        return float(value)
    except (TypeError, ValueError):
        return value


def empty_string_mask(series: pd.Series) -> pd.Series:
    """
    Treat null or whitespace-only string as empty.
    """

    return (
        series.isna()
        | series.fillna("").astype(str).str.strip().eq("")
    )


def check_required_columns(
    actual_columns,
    required_columns,
):
    actual = set(actual_columns)

    missing = [
        col
        for col in required_columns
        if col not in actual
    ]

    return missing


def add_failure(
    failures: list,
    dataset: str,
    rule: str,
    count: int | None = None,
):
    item = {
        "dataset": dataset,
        "rule": rule,
    }

    if count is not None:
        item["count"] = int(count)

    failures.append(item)


# ============================================================
# MOVIELENS: RATINGS
# ============================================================

def profile_ratings(
    path: Path,
    failures: list,
) -> dict:

    print("[PROFILE] MovieLens ratings.csv")

    df = pd.read_csv(
        path,
        dtype=str,
    )

    missing_columns = check_required_columns(
        df.columns,
        RATINGS_COLUMNS,
    )

    result = {
        "path": str(path.relative_to(PROJECT_ROOT)),
        "row_count": int(len(df)),
        "missing_columns": missing_columns,
    }

    if missing_columns:

        add_failure(
            failures,
            "ratings.csv",
            f"missing required columns: {missing_columns}",
        )

        return result

    # --------------------------------------------------------
    # Convert for validation only.
    # Raw DataFrame/file is NOT modified.
    # --------------------------------------------------------

    user_id = pd.to_numeric(
        df["userId"],
        errors="coerce",
    )

    movie_id = pd.to_numeric(
        df["movieId"],
        errors="coerce",
    )

    rating = pd.to_numeric(
        df["rating"],
        errors="coerce",
    )

    timestamp_numeric = pd.to_numeric(
        df["timestamp"],
        errors="coerce",
    )

    timestamp_dt = pd.to_datetime(
        timestamp_numeric,
        unit="s",
        utc=True,
        errors="coerce",
    )

    invalid_user = (
        user_id.isna()
        | (user_id <= 0)
    )

    invalid_movie = (
        movie_id.isna()
        | (movie_id <= 0)
    )

    invalid_rating = (
        rating.isna()
        | (rating < 0.5)
        | (rating > 5.0)
    )

    invalid_timestamp = (
        timestamp_dt.isna()
    )

    result.update(
        {
            "distinct_user_count": int(
                user_id.dropna().nunique()
            ),
            "distinct_movie_count": int(
                movie_id.dropna().nunique()
            ),
            "min_rating": safe_number(
                rating.min()
            ),
            "max_rating": safe_number(
                rating.max()
            ),
            "min_timestamp": (
                timestamp_dt.min().isoformat()
                if timestamp_dt.notna().any()
                else None
            ),
            "max_timestamp": (
                timestamp_dt.max().isoformat()
                if timestamp_dt.notna().any()
                else None
            ),
            "invalid_user_id_count": int(
                invalid_user.sum()
            ),
            "invalid_movie_id_count": int(
                invalid_movie.sum()
            ),
            "invalid_rating_count": int(
                invalid_rating.sum()
            ),
            "invalid_timestamp_count": int(
                invalid_timestamp.sum()
            ),
        }
    )

    if invalid_user.any():
        add_failure(
            failures,
            "ratings.csv",
            "userId must be numeric and > 0",
            invalid_user.sum(),
        )

    if invalid_movie.any():
        add_failure(
            failures,
            "ratings.csv",
            "movieId must be numeric and > 0",
            invalid_movie.sum(),
        )

    if invalid_rating.any():
        add_failure(
            failures,
            "ratings.csv",
            "rating must be between 0.5 and 5.0",
            invalid_rating.sum(),
        )

    if invalid_timestamp.any():
        add_failure(
            failures,
            "ratings.csv",
            "timestamp must be parseable Unix seconds",
            invalid_timestamp.sum(),
        )

    return result


# ============================================================
# MOVIELENS: MOVIES
# ============================================================

def profile_movies(
    path: Path,
    failures: list,
) -> dict:

    print("[PROFILE] MovieLens movies.csv")

    df = pd.read_csv(
        path,
        dtype=str,
    )

    missing_columns = check_required_columns(
        df.columns,
        MOVIES_COLUMNS,
    )

    result = {
        "path": str(path.relative_to(PROJECT_ROOT)),
        "row_count": int(len(df)),
        "missing_columns": missing_columns,
    }

    if missing_columns:

        add_failure(
            failures,
            "movies.csv",
            f"missing required columns: {missing_columns}",
        )

        return result

    movie_id = pd.to_numeric(
        df["movieId"],
        errors="coerce",
    )

    invalid_movie_id = (
        movie_id.isna()
        | (movie_id <= 0)
    )

    duplicate_movie_id = (
        movie_id.notna()
        & movie_id.duplicated(
            keep=False
        )
    )

    empty_title = empty_string_mask(
        df["title"]
    )

    empty_genres = empty_string_mask(
        df["genres"]
    )

    no_genres_listed = (
        df["genres"]
        .fillna("")
        .astype(str)
        .str.strip()
        .eq("(no genres listed)")
    )

    result.update(
        {
            "distinct_movie_count": int(
                movie_id.dropna().nunique()
            ),
            "invalid_movie_id_count": int(
                invalid_movie_id.sum()
            ),
            "duplicate_movie_id_count": int(
                duplicate_movie_id.sum()
            ),
            "empty_title_count": int(
                empty_title.sum()
            ),
            "empty_genres_count": int(
                empty_genres.sum()
            ),
            "no_genres_listed_count": int(
                no_genres_listed.sum()
            ),
        }
    )

    if invalid_movie_id.any():
        add_failure(
            failures,
            "movies.csv",
            "movieId must be numeric and > 0",
            invalid_movie_id.sum(),
        )

    if duplicate_movie_id.any():
        add_failure(
            failures,
            "movies.csv",
            "movieId must be unique",
            duplicate_movie_id.sum(),
        )

    if empty_title.any():
        add_failure(
            failures,
            "movies.csv",
            "title must not be empty",
            empty_title.sum(),
        )

    if empty_genres.any():
        add_failure(
            failures,
            "movies.csv",
            "genres must not be empty",
            empty_genres.sum(),
        )

    return result


# ============================================================
# MOVIELENS: LINKS
# ============================================================

def profile_links(
    path: Path,
    failures: list,
) -> dict:

    print("[PROFILE] MovieLens links.csv")

    df = pd.read_csv(
        path,
        dtype=str,
    )

    missing_columns = check_required_columns(
        df.columns,
        LINKS_COLUMNS,
    )

    result = {
        "path": str(path.relative_to(PROJECT_ROOT)),
        "row_count": int(len(df)),
        "missing_columns": missing_columns,
    }

    if missing_columns:

        add_failure(
            failures,
            "links.csv",
            f"missing required columns: {missing_columns}",
        )

        return result

    # imdbId / tmdbId are explicitly allowed to be null
    # in Step 5. We only profile them here.

    result.update(
        {
            "null_movie_id_count": int(
                df["movieId"].isna().sum()
            ),
            "null_imdb_id_count": int(
                df["imdbId"].isna().sum()
            ),
            "null_tmdb_id_count": int(
                df["tmdbId"].isna().sum()
            ),
            "distinct_movie_id_count": int(
                df["movieId"].dropna().nunique()
            ),
        }
    )

    return result


# ============================================================
# IMDb HEADER CHECK
# ============================================================

def read_imdb_header(
    path: Path,
) -> list[str]:

    header = pd.read_csv(
        path,
        sep="\t",
        compression="gzip",
        nrows=0,
        dtype=str,
    )

    return list(header.columns)


# ============================================================
# IMDb: TITLE.BASICS
# ============================================================

def profile_imdb_basics(
    path: Path,
    failures: list,
) -> dict:

    print("[PROFILE] IMDb title.basics.tsv.gz")

    columns = read_imdb_header(
        path
    )

    missing_columns = check_required_columns(
        columns,
        IMDB_BASICS_COLUMNS,
    )

    result = {
        "path": str(path.relative_to(PROJECT_ROOT)),
        "missing_columns": missing_columns,
    }

    if missing_columns:

        add_failure(
            failures,
            "title.basics.tsv.gz",
            f"missing required columns: {missing_columns}",
        )

        return result

    row_count = 0

    null_counts = {
        col: 0
        for col in IMDB_BASICS_COLUMNS
    }

    movie_title_count = 0

    # IMPORTANT:
    # Only required columns are loaded.
    # File remains compressed on disk.

    for chunk in pd.read_csv(
        path,
        sep="\t",
        compression="gzip",
        dtype=str,
        na_values="\\N",
        usecols=IMDB_BASICS_COLUMNS,
        chunksize=IMDB_CHUNK_SIZE,
        low_memory=False,
    ):

        row_count += len(chunk)

        for col in IMDB_BASICS_COLUMNS:
            null_counts[col] += int(
                chunk[col].isna().sum()
            )

        movie_title_count += int(
            chunk["titleType"]
            .fillna("")
            .eq("movie")
            .sum()
        )

    result.update(
        {
            "row_count": int(row_count),
            "movie_title_count": int(
                movie_title_count
            ),
            "null_counts": null_counts,
        }
    )

    return result


# ============================================================
# IMDb: TITLE.RATINGS
# ============================================================

def profile_imdb_ratings(
    path: Path,
    failures: list,
) -> dict:

    print("[PROFILE] IMDb title.ratings.tsv.gz")

    columns = read_imdb_header(
        path
    )

    missing_columns = check_required_columns(
        columns,
        IMDB_RATINGS_COLUMNS,
    )

    result = {
        "path": str(path.relative_to(PROJECT_ROOT)),
        "missing_columns": missing_columns,
    }

    if missing_columns:

        add_failure(
            failures,
            "title.ratings.tsv.gz",
            f"missing required columns: {missing_columns}",
        )

        return result

    row_count = 0
    null_counts = {
        col: 0
        for col in IMDB_RATINGS_COLUMNS
    }

    invalid_average_rating_count = 0
    invalid_num_votes_count = 0

    min_rating = None
    max_rating = None

    min_votes = None
    max_votes = None

    for chunk in pd.read_csv(
        path,
        sep="\t",
        compression="gzip",
        dtype=str,
        na_values="\\N",
        usecols=IMDB_RATINGS_COLUMNS,
        chunksize=IMDB_CHUNK_SIZE,
        low_memory=False,
    ):

        row_count += len(chunk)

        for col in IMDB_RATINGS_COLUMNS:
            null_counts[col] += int(
                chunk[col].isna().sum()
            )

        average_rating = pd.to_numeric(
            chunk["averageRating"],
            errors="coerce",
        )

        num_votes = pd.to_numeric(
            chunk["numVotes"],
            errors="coerce",
        )

        invalid_average_rating_count += int(
            (
                chunk["averageRating"].notna()
                & average_rating.isna()
            ).sum()
        )

        invalid_num_votes_count += int(
            (
                chunk["numVotes"].notna()
                & num_votes.isna()
            ).sum()
        )

        if average_rating.notna().any():

            current_min = average_rating.min()
            current_max = average_rating.max()

            min_rating = (
                current_min
                if min_rating is None
                else min(
                    min_rating,
                    current_min,
                )
            )

            max_rating = (
                current_max
                if max_rating is None
                else max(
                    max_rating,
                    current_max,
                )
            )

        if num_votes.notna().any():

            current_min = num_votes.min()
            current_max = num_votes.max()

            min_votes = (
                current_min
                if min_votes is None
                else min(
                    min_votes,
                    current_min,
                )
            )

            max_votes = (
                current_max
                if max_votes is None
                else max(
                    max_votes,
                    current_max,
                )
            )

    result.update(
        {
            "row_count": int(row_count),
            "null_counts": null_counts,
            "invalid_average_rating_count":
                invalid_average_rating_count,
            "invalid_num_votes_count":
                invalid_num_votes_count,
            "min_average_rating":
                safe_number(min_rating),
            "max_average_rating":
                safe_number(max_rating),
            "min_num_votes":
                safe_number(min_votes),
            "max_num_votes":
                safe_number(max_votes),
        }
    )

    # These two are diagnostics only in Step 5.
    # DATA_CONTRACT does not define them as hard-fail
    # conditions for the raw IMDb source.

    return result


# ============================================================
# MAIN
# ============================================================

def main() -> None:

    print("=" * 60)
    print("Source Data Profiler")
    print("=" * 60)

    paths = {
        "ratings": (
            MOVIELENS_DIR
            / "ratings.csv"
        ),
        "movies": (
            MOVIELENS_DIR
            / "movies.csv"
        ),
        "links": (
            MOVIELENS_DIR
            / "links.csv"
        ),
        "imdb_basics": (
            IMDB_DIR
            / "title.basics.tsv.gz"
        ),
        "imdb_ratings": (
            IMDB_DIR
            / "title.ratings.tsv.gz"
        ),
    }

    missing_files = [
        str(path)
        for path in paths.values()
        if not path.is_file()
    ]

    if missing_files:

        print("[FAIL] Missing source files:")

        for path in missing_files:
            print(f"  - {path}")

        sys.exit(1)

    REPORT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    failures = []

    report = {
        "generated_at": utc_now(),
        "movielens_variant":
            MOVIELENS_VARIANT,
        "status": None,
        "sources": {},
        "hard_failures": failures,
    }

    report["sources"]["ratings"] = (
        profile_ratings(
            paths["ratings"],
            failures,
        )
    )

    report["sources"]["movies"] = (
        profile_movies(
            paths["movies"],
            failures,
        )
    )

    report["sources"]["links"] = (
        profile_links(
            paths["links"],
            failures,
        )
    )

    report["sources"]["imdb_basics"] = (
        profile_imdb_basics(
            paths["imdb_basics"],
            failures,
        )
    )

    report["sources"]["imdb_ratings"] = (
        profile_imdb_ratings(
            paths["imdb_ratings"],
            failures,
        )
    )

    report["status"] = (
        "PASS"
        if not failures
        else "FAIL"
    )

    # Atomic report write
    temp_path = REPORT_PATH.with_suffix(
        ".json.tmp"
    )

    with temp_path.open(
        "w",
        encoding="utf-8",
    ) as f:

        json.dump(
            report,
            f,
            indent=2,
            ensure_ascii=False,
        )

    temp_path.replace(
        REPORT_PATH
    )

    print()
    print("=" * 60)
    print(f"STATUS: {report['status']}")
    print("=" * 60)

    print(
        f"Report: {REPORT_PATH}"
    )

    if failures:

        print()
        print("Hard failures:")

        for failure in failures:
            print(
                f"  - {failure}"
            )

        sys.exit(1)

    print()
    print("[OK] Source profiling completed.")


if __name__ == "__main__":
    main()
