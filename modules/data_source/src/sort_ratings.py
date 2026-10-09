from __future__ import annotations

import hashlib
import os
import sys

from collections import Counter
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

INPUT_PATH = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "movielens"
    / MOVIELENS_VARIANT
    / "ratings.csv"
)

OUTPUT_DIR = (
    PROJECT_ROOT
    / "data"
    / "prepared"
    / "movielens"
)

OUTPUT_PATH = (
    OUTPUT_DIR
    / "ratings_sorted.csv"
)


REQUIRED_COLUMNS = [
    "userId",
    "movieId",
    "rating",
    "timestamp",
]


def sha256_file(
    path: Path,
) -> str:

    sha256 = hashlib.sha256()

    with path.open("rb") as f:

        while True:

            chunk = f.read(
                1024 * 1024
            )

            if not chunk:
                break

            sha256.update(
                chunk
            )

    return sha256.hexdigest()

def load_ratings() -> pd.DataFrame:

    if not INPUT_PATH.is_file():

        raise RuntimeError(
            "ratings.csv not found: "
            f"{INPUT_PATH}"
        )

    df = pd.read_csv(
        INPUT_PATH,
        dtype=str,
        keep_default_na=False,
    )

    missing = [
        col
        for col in REQUIRED_COLUMNS
        if col not in df.columns
    ]

    if missing:

        raise RuntimeError(
            "ratings.csv missing columns: "
            f"{missing}"
        )

    return df

def sort_ratings(
    df: pd.DataFrame,
) -> pd.DataFrame:

    result = df.copy()

    # Temporary helper only.
    result["_timestamp_numeric"] = (
        pd.to_numeric(
            result["timestamp"],
            errors="raise",
        )
    )

    # Stable global sort.
    result = result.sort_values(
        by="_timestamp_numeric",
        ascending=True,
        kind="mergesort",
    )

    result = result.drop(
        columns=[
            "_timestamp_numeric"
        ]
    )

    result = result.reset_index(
        drop=True
    )

    return result

def validate_row_count(
    source: pd.DataFrame,
    output: pd.DataFrame,
) -> None:

    if len(source) != len(output):

        raise RuntimeError(
            "Row count changed after sorting: "
            f"input={len(source)}, "
            f"output={len(output)}"
        )

def validate_monotonic_timestamp(
    output: pd.DataFrame,
) -> None:

    timestamp = pd.to_numeric(
        output["timestamp"],
        errors="raise",
    )

    if not timestamp.is_monotonic_increasing:

        raise RuntimeError(
            "Output timestamp is not "
            "monotonic non-decreasing."
        )

def validate_timestamp_range(
    source: pd.DataFrame,
    output: pd.DataFrame,
) -> None:

    source_ts = pd.to_numeric(
        source["timestamp"],
        errors="raise",
    )

    output_ts = pd.to_numeric(
        output["timestamp"],
        errors="raise",
    )

    source_min = int(
        source_ts.min()
    )

    source_max = int(
        source_ts.max()
    )

    output_min = int(
        output_ts.min()
    )

    output_max = int(
        output_ts.max()
    )

    if source_min != output_min:

        raise RuntimeError(
            "Minimum timestamp changed: "
            f"{source_min} -> {output_min}"
        )

    if source_max != output_max:

        raise RuntimeError(
            "Maximum timestamp changed: "
            f"{source_max} -> {output_max}"
        )

def validate_rating_unchanged(
    source: pd.DataFrame,
    output: pd.DataFrame,
) -> None:

    input_ratings = Counter(
        source["rating"].tolist()
    )

    output_ratings = Counter(
        output["rating"].tolist()
    )

    if input_ratings != output_ratings:

        raise RuntimeError(
            "Rating values changed "
            "during sorting."
        )

def row_multiset(
    df: pd.DataFrame,
) -> Counter:

    columns = list(
        df.columns
    )

    return Counter(
        tuple(row)
        for row in df[
            columns
        ].itertuples(
            index=False,
            name=None,
        )
    )


def validate_rows_unchanged(
    source: pd.DataFrame,
    output: pd.DataFrame,
) -> None:

    if list(source.columns) != list(
        output.columns
    ):

        raise RuntimeError(
            "Output columns differ "
            "from input columns."
        )

    if row_multiset(
        source
    ) != row_multiset(
        output
    ):

        raise RuntimeError(
            "Row contents changed. "
            "Step 11 may only reorder rows."
        )

def write_sorted_ratings(
    df: pd.DataFrame,
) -> None:

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    temp_path = (
        OUTPUT_DIR
        / "ratings_sorted.csv.tmp"
    )

    df.to_csv(
        temp_path,
        index=False,
        encoding="utf-8",
        lineterminator="\n",
    )

    os.replace(
        temp_path,
        OUTPUT_PATH,
    )

def load_written_output() -> pd.DataFrame:

    return pd.read_csv(
        OUTPUT_PATH,
        dtype=str,
        keep_default_na=False,
    )

def validate_output(
    source: pd.DataFrame,
    output: pd.DataFrame,
) -> None:

    validate_row_count(
        source,
        output,
    )

    validate_monotonic_timestamp(
        output
    )

    validate_timestamp_range(
        source,
        output,
    )

    validate_rating_unchanged(
        source,
        output,
    )

    validate_rows_unchanged(
        source,
        output,
    )

def main() -> None:

    print("=" * 60)
    print("Prepare MovieLens Ratings for Kafka Replay")
    print("=" * 60)

    # --------------------------------------------------------
    # 1. Protect raw integrity
    # --------------------------------------------------------

    raw_sha_before = sha256_file(
        INPUT_PATH
    )

    print(
        "[RAW] SHA256 before:",
        raw_sha_before,
    )

    # --------------------------------------------------------
    # 2. Read raw
    # --------------------------------------------------------

    source = load_ratings()

    print(
        f"[INPUT] rows: {len(source)}"
    )

    input_timestamp = pd.to_numeric(
        source["timestamp"],
        errors="raise",
    )

    print(
        "[INPUT] min timestamp:",
        int(input_timestamp.min()),
    )

    print(
        "[INPUT] max timestamp:",
        int(input_timestamp.max()),
    )

    # --------------------------------------------------------
    # 3. Sort globally
    # --------------------------------------------------------

    sorted_df = sort_ratings(
        source
    )

    # --------------------------------------------------------
    # 4. Write prepared artifact
    # --------------------------------------------------------

    write_sorted_ratings(
        sorted_df
    )

    # --------------------------------------------------------
    # 5. Re-read actual output
    # --------------------------------------------------------

    output = (
        load_written_output()
    )

    # --------------------------------------------------------
    # 6. Validate
    # --------------------------------------------------------

    validate_output(
        source,
        output,
    )

    # --------------------------------------------------------
    # 7. Ensure raw did not change
    # --------------------------------------------------------

    raw_sha_after = sha256_file(
        INPUT_PATH
    )

    if raw_sha_before != raw_sha_after:

        raise RuntimeError(
            "Raw ratings.csv was modified!"
        )

    # --------------------------------------------------------
    # 8. Output checksum
    # --------------------------------------------------------

    output_sha256 = sha256_file(
        OUTPUT_PATH
    )

    output_timestamp = pd.to_numeric(
        output["timestamp"],
        errors="raise",
    )

    print()
    print("=" * 60)
    print("STEP 11 PASSED")
    print("=" * 60)

    print(
        f"Output: {OUTPUT_PATH}"
    )

    print(
        f"Rows: {len(output)}"
    )

    print(
        "Timestamp monotonic:",
        output_timestamp
        .is_monotonic_increasing,
    )

    print(
        "Min timestamp:",
        int(output_timestamp.min()),
    )

    print(
        "Max timestamp:",
        int(output_timestamp.max()),
    )

    print(
        "Raw unchanged:",
        raw_sha_before
        == raw_sha_after,
    )

    print(
        "Output SHA256:",
        output_sha256,
    )


if __name__ == "__main__":

    try:
        main()

    except Exception as exc:

        print()
        print(
            f"[FAIL] {exc}"
        )

        sys.exit(1)


