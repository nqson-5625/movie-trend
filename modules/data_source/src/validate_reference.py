from __future__ import annotations

import json
import os
import re
import sys

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv


# ============================================================
# CONFIG
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[3]

load_dotenv(PROJECT_ROOT / ".env")

MOVIELENS_VARIANT = os.getenv(
    "MOVIELENS_VARIANT",
    "ml-latest-small",
)

MOVIES_PATH = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "movielens"
    / MOVIELENS_VARIANT
    / "movies.csv"
)

IMDB_BASICS_PATH = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "imdb"
    / "title.basics.tsv.gz"
)

REFERENCE_PATH = (
    PROJECT_ROOT
    / "data"
    / "prepared"
    / "reference"
    / "movie_reference.tsv"
)

REPORT_DIR = (
    PROJECT_ROOT
    / "data"
    / "reports"
)

REPORT_PATH = (
    REPORT_DIR
    / "data_quality_report.json"
)

IMDB_CHUNK_SIZE = 200_000


# ============================================================
# CONTRACT
# ============================================================

EXPECTED_COLUMNS = [
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

IMDB_ID_PATTERN = re.compile(
    r"^tt[0-9]+$"
)


# ============================================================
# IMPLEMENTATION RULE
# ============================================================
#
# MODULE_01 says "valid range" but does not
# specify numeric bounds.
#
# 0..10 is used here as the project implementation
# rule for IMDb averageRating.
# Freeze this in DATA_CONTRACT if not already done.
# ============================================================

IMDB_RATING_MIN = 0.0
IMDB_RATING_MAX = 10.0


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


def empty_mask(
    series: pd.Series,
) -> pd.Series:

    return (
        series.isna()
        | series
        .fillna("")
        .astype(str)
        .str.strip()
        .eq("")
    )


def add_failure(
    failures: list,
    rule: str,
    count: int,
) -> None:

    if count <= 0:
        return

    failures.append(
        {
            "rule": rule,
            "count": int(count),
        }
    )


def percentage(
    count: int,
    total: int,
) -> float:

    if total == 0:
        return 0.0

    return round(
        count / total * 100,
        4,
    )

def load_reference() -> pd.DataFrame:

    if not REFERENCE_PATH.is_file():

        raise RuntimeError(
            "movie_reference.tsv not found: "
            f"{REFERENCE_PATH}"
        )

    return pd.read_csv(
        REFERENCE_PATH,
        sep="\t",
        dtype=str,
        keep_default_na=True,
    )

def validate_schema(
    df: pd.DataFrame,
    failures: list,
) -> None:

    actual = list(
        df.columns
    )

    missing = [
        col
        for col in EXPECTED_COLUMNS
        if col not in actual
    ]

    unexpected = [
        col
        for col in actual
        if col not in EXPECTED_COLUMNS
    ]

    if missing:

        failures.append(
            {
                "rule":
                    "missing canonical columns",
                "columns":
                    missing,
            }
        )

    if unexpected:

        failures.append(
            {
                "rule":
                    "unexpected canonical columns",
                "columns":
                    unexpected,
            }
        )

def validate_movie_id(
    df: pd.DataFrame,
    failures: list,
) -> None:

    raw = df["movieId"]

    numeric = pd.to_numeric(
        raw,
        errors="coerce",
    )

    null_or_invalid = (
        numeric.isna()
    )

    non_positive = (
        numeric.notna()
        & (numeric <= 0)
    )

    non_integer = (
        numeric.notna()
        & (numeric % 1 != 0)
    )

    duplicate = (
        numeric.notna()
        & numeric.duplicated(
            keep=False
        )
    )

    add_failure(
        failures,
        "movieId must not be null "
        "and must be numeric",
        null_or_invalid.sum(),
    )

    add_failure(
        failures,
        "movieId must be > 0",
        non_positive.sum(),
    )

    add_failure(
        failures,
        "movieId must be integer",
        non_integer.sum(),
    )

    add_failure(
        failures,
        "movieId must be unique",
        duplicate.sum(),
    )

def validate_required_text(
    df: pd.DataFrame,
    failures: list,
) -> None:

    empty_title = empty_mask(
        df["title"]
    )

    empty_genres = empty_mask(
        df["genres"]
    )

    add_failure(
        failures,
        "title must not be null/empty",
        empty_title.sum(),
    )

    add_failure(
        failures,
        "genres must not be null/empty",
        empty_genres.sum(),
    )

def validate_imdb_id(
    df: pd.DataFrame,
    failures: list,
) -> None:

    values = (
        df["imdbId"]
        .dropna()
        .astype(str)
        .str.strip()
    )

    invalid = ~values.str.match(
        IMDB_ID_PATTERN
    )

    add_failure(
        failures,
        "imdbId must match ^tt[0-9]+$ "
        "when non-null",
        invalid.sum(),
    )

def validate_tmdb_id(
    df: pd.DataFrame,
    failures: list,
) -> None:

    present = (
        df["tmdbId"]
        .notna()
    )

    numeric = pd.to_numeric(
        df["tmdbId"],
        errors="coerce",
    )

    invalid_numeric = (
        present
        & numeric.isna()
    )

    non_positive = (
        present
        & numeric.notna()
        & (numeric <= 0)
    )

    non_integer = (
        present
        & numeric.notna()
        & (numeric % 1 != 0)
    )

    add_failure(
        failures,
        "tmdbId must be numeric when non-null",
        invalid_numeric.sum(),
    )

    add_failure(
        failures,
        "tmdbId must be > 0 when non-null",
        non_positive.sum(),
    )

    add_failure(
        failures,
        "tmdbId must be integer when non-null",
        non_integer.sum(),
    )

def validate_imdb_rating(
    df: pd.DataFrame,
    failures: list,
) -> None:

    present = (
        df["imdb_rating"]
        .notna()
    )

    numeric = pd.to_numeric(
        df["imdb_rating"],
        errors="coerce",
    )

    invalid_numeric = (
        present
        & numeric.isna()
    )

    out_of_range = (
        present
        & numeric.notna()
        & (
            (numeric < IMDB_RATING_MIN)
            | (
                numeric
                > IMDB_RATING_MAX
            )
        )
    )

    add_failure(
        failures,
        "imdb_rating must be numeric "
        "when non-null",
        invalid_numeric.sum(),
    )

    add_failure(
        failures,
        (
            "imdb_rating outside "
            f"[{IMDB_RATING_MIN}, "
            f"{IMDB_RATING_MAX}]"
        ),
        out_of_range.sum(),
    )

def validate_imdb_votes(
    df: pd.DataFrame,
    failures: list,
) -> None:

    present = (
        df["imdb_votes"]
        .notna()
    )

    numeric = pd.to_numeric(
        df["imdb_votes"],
        errors="coerce",
    )

    invalid_numeric = (
        present
        & numeric.isna()
    )

    negative = (
        present
        & numeric.notna()
        & (numeric < 0)
    )

    non_integer = (
        present
        & numeric.notna()
        & (numeric % 1 != 0)
    )

    add_failure(
        failures,
        "imdb_votes must be numeric "
        "when non-null",
        invalid_numeric.sum(),
    )

    add_failure(
        failures,
        "imdb_votes must be >= 0",
        negative.sum(),
    )

    add_failure(
        failures,
        "imdb_votes must be integer",
        non_integer.sum(),
    )

def validate_row_count(
    reference: pd.DataFrame,
    failures: list,
) -> dict:

    movies = pd.read_csv(
        MOVIES_PATH,
        dtype=str,
    )

    reference_rows = len(
        reference
    )

    movies_rows = len(
        movies
    )

    if reference_rows != movies_rows:

        failures.append(
            {
                "rule":
                    "rows(movie_reference) "
                    "must equal rows(movies.csv)",
                "movie_reference_rows":
                    int(reference_rows),
                "movies_rows":
                    int(movies_rows),
            }
        )

    return {
        "movie_reference_rows":
            int(reference_rows),
        "movies_rows":
            int(movies_rows),
        "equal":
            reference_rows
            == movies_rows,
    }

def find_matched_imdb_basic_ids(
    target_ids: set[str],
) -> set[str]:

    matched = set()

    if not target_ids:
        return matched

    for chunk in pd.read_csv(
        IMDB_BASICS_PATH,
        sep="\t",
        compression="gzip",
        dtype=str,
        usecols=["tconst"],
        chunksize=IMDB_CHUNK_SIZE,
        low_memory=False,
    ):

        hits = chunk[
            chunk["tconst"].isin(
                target_ids
            )
        ]

        matched.update(
            hits["tconst"]
            .dropna()
            .tolist()
        )

    return matched

def calculate_coverage(
    df: pd.DataFrame,
) -> dict:

    total = len(df)

    has_imdb_id = int(
        df["imdbId"]
        .notna()
        .sum()
    )

    has_tmdb_id = int(
        df["tmdbId"]
        .notna()
        .sum()
    )

    has_imdb_rating = int(
        df["imdb_rating"]
        .notna()
        .sum()
    )

    missing_release_year = int(
        df["release_year"]
        .isna()
        .sum()
    )

    no_genres_listed = int(
        df["genres"]
        .fillna("")
        .astype(str)
        .str.strip()
        .eq("(no genres listed)")
        .sum()
    )

    target_imdb_ids = set(
        df["imdbId"]
        .dropna()
        .astype(str)
    )

    matched_ids = (
        find_matched_imdb_basic_ids(
            target_imdb_ids
        )
    )

    matched_imdb_basics = int(
        df["imdbId"]
        .isin(matched_ids)
        .sum()
    )

    return {
        "total_movies": int(total),

        "has_imdb_id": {
            "count": has_imdb_id,
            "percent": percentage(
                has_imdb_id,
                total,
            ),
        },

        "has_tmdb_id": {
            "count": has_tmdb_id,
            "percent": percentage(
                has_tmdb_id,
                total,
            ),
        },

        "matched_imdb_basics": {
            "count":
                matched_imdb_basics,
            "percent":
                percentage(
                    matched_imdb_basics,
                    total,
                ),
        },

        "has_imdb_rating": {
            "count":
                has_imdb_rating,
            "percent":
                percentage(
                    has_imdb_rating,
                    total,
                ),
        },

        "missing_release_year": {
            "count":
                missing_release_year,
            "percent":
                percentage(
                    missing_release_year,
                    total,
                ),
        },

        "no_genres_listed": {
            "count":
                no_genres_listed,
            "percent":
                percentage(
                    no_genres_listed,
                    total,
                ),
        },
    }

def validate_release_year_format(
    df: pd.DataFrame,
    failures: list,
) -> None:

    present = (
        df["release_year"]
        .notna()
    )

    numeric = pd.to_numeric(
        df["release_year"],
        errors="coerce",
    )

    invalid = (
        present
        & numeric.isna()
    )

    add_failure(
        failures,
        "release_year must be numeric "
        "when non-null",
        invalid.sum(),
    )

def validate_build_metadata(
    df: pd.DataFrame,
    failures: list,
) -> None:

    empty_version = empty_mask(
        df["ref_version"]
    )

    empty_generated_at = empty_mask(
        df["generated_at"]
    )

    add_failure(
        failures,
        "ref_version must not be empty",
        empty_version.sum(),
    )

    add_failure(
        failures,
        "generated_at must not be empty",
        empty_generated_at.sum(),
    )

    if not empty_version.any():

        version_count = (
            df["ref_version"]
            .nunique()
        )

        if version_count != 1:

            failures.append(
                {
                    "rule":
                        "one build must contain "
                        "exactly one ref_version",
                    "distinct_count":
                        int(version_count),
                }
            )

    if not empty_generated_at.any():

        generated_count = (
            df["generated_at"]
            .nunique()
        )

        if generated_count != 1:

            failures.append(
                {
                    "rule":
                        "one build must contain "
                        "exactly one generated_at",
                    "distinct_count":
                        int(generated_count),
                }
            )

def validate_reference() -> dict:

    print("=" * 60)
    print("Validate movie_reference")
    print("=" * 60)

    failures = []

    df = load_reference()

    # --------------------------------------------------------
    # Schema first
    # --------------------------------------------------------

    validate_schema(
        df,
        failures,
    )

    missing_required_columns = (
        set(EXPECTED_COLUMNS)
        - set(df.columns)
    )

    # Cannot safely continue field-level validation
    # if canonical columns are missing.
    if missing_required_columns:

        return {
            "generated_at":
                utc_now(),
            "status":
                "FAIL",
            "hard_failures":
                failures,
            "coverage":
                None,
        }

    # --------------------------------------------------------
    # Contract hard checks
    # --------------------------------------------------------

    validate_movie_id(
        df,
        failures,
    )

    validate_required_text(
        df,
        failures,
    )

    validate_imdb_id(
        df,
        failures,
    )

    validate_tmdb_id(
        df,
        failures,
    )

    validate_imdb_rating(
        df,
        failures,
    )

    validate_imdb_votes(
        df,
        failures,
    )

    # --------------------------------------------------------
    # Additional structural guards
    # --------------------------------------------------------

    validate_release_year_format(
        df,
        failures,
    )

    validate_build_metadata(
        df,
        failures,
    )

    # --------------------------------------------------------
    # Invariant
    # --------------------------------------------------------

    row_count_check = (
        validate_row_count(
            df,
            failures,
        )
    )

    # --------------------------------------------------------
    # Coverage — report only
    # --------------------------------------------------------

    coverage = (
        calculate_coverage(
            df
        )
    )

    status = (
        "PASS"
        if not failures
        else "FAIL"
    )

    return {
        "generated_at":
            utc_now(),

        "reference_path":
            str(
                REFERENCE_PATH.relative_to(
                    PROJECT_ROOT
                )
            ),

        "status":
            status,

        "row_count_check":
            row_count_check,

        "hard_failures":
            failures,

        "coverage":
            coverage,
    }

def write_report(
    report: dict,
) -> None:

    REPORT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    temp_path = (
        REPORT_PATH.with_suffix(
            ".json.tmp"
        )
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

    os.replace(
        temp_path,
        REPORT_PATH,
    )

def main() -> None:

    report = (
        validate_reference()
    )

    write_report(
        report
    )

    print()
    print("=" * 60)
    print(
        f"STATUS: {report['status']}"
    )
    print("=" * 60)

    print(
        f"Report: {REPORT_PATH}"
    )

    if report["coverage"]:

        print()
        print("Coverage:")

        for key, value in (
            report["coverage"].items()
        ):

            if key == "total_movies":
                print(
                    f"  total_movies: "
                    f"{value}"
                )

                continue

            print(
                f"  {key}: "
                f"{value['count']} "
                f"({value['percent']}%)"
            )

    if report["hard_failures"]:

        print()
        print("Hard failures:")

        for failure in (
            report["hard_failures"]
        ):

            print(
                f"  - {failure}"
            )

        sys.exit(1)

    print()
    print(
        "[OK] movie_reference "
        "passed quality gate."
    )


if __name__ == "__main__":
    main()


