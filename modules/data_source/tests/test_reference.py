from __future__ import annotations

import hashlib
import json
import re

import pandas as pd
import pytest

import modules.data_source.src.build_movie_reference as bmr


# ============================================================
# TEST FIXTURES
# ============================================================

@pytest.fixture
def movies_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "movieId": [1, 2, 3],
            "title": [
                "Movie A (1999)",
                "Movie B (2001)",
                "Movie C",
            ],
            "genres": [
                "Drama",
                "Comedy|Romance",
                "(no genres listed)",
            ],
        }
    )


@pytest.fixture
def links_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "movieId": [1, 2],
            "imdbId": pd.Series(
                [
                    "tt0000001",
                    "tt0000002",
                ],
                dtype="string",
            ),
            "tmdbId": pd.Series(
                [
                    10,
                    20,
                ],
                dtype="Int64",
            ),
        }
    )


@pytest.fixture
def imdb_basics_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "imdbId": [
                "tt0000001",
            ],
            "titleType": [
                "movie",
            ],
            "primaryTitle": [
                "Movie A",
            ],
            "startYear": [
                "2000",
            ],
        }
    )


@pytest.fixture
def imdb_ratings_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "imdbId": [
                "tt0000001",
            ],
            "averageRating": [
                "8.1",
            ],
            "numVotes": [
                "12345",
            ],
        }
    )


@pytest.fixture
def joined_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "movieId": [1, 2, 3],
            "title": [
                "Movie A (1999)",
                "Movie B (2001)",
                "Movie C",
            ],
            "genres": [
                "Drama",
                "Comedy|Romance",
                "(no genres listed)",
            ],
            "imdbId": pd.Series(
                [
                    "tt0000001",
                    "tt0000002",
                    pd.NA,
                ],
                dtype="string",
            ),
            "tmdbId": pd.Series(
                [
                    10,
                    20,
                    pd.NA,
                ],
                dtype="Int64",
            ),
            "titleType": [
                "movie",
                pd.NA,
                pd.NA,
            ],
            "primaryTitle": [
                "Movie A",
                pd.NA,
                pd.NA,
            ],
            "startYear": [
                "2000",
                pd.NA,
                pd.NA,
            ],
            "averageRating": [
                "8.1",
                pd.NA,
                pd.NA,
            ],
            "numVotes": [
                "12345",
                pd.NA,
                pd.NA,
            ],
        }
    )


@pytest.fixture
def source_manifest() -> dict:
    return {
        "sources": [
            {
                "source_name": "MovieLens",
                "files": [
                    {
                        "name": "movies.csv",
                        "sha256": "aaa111",
                    },
                    {
                        "name": "links.csv",
                        "sha256": "bbb222",
                    },
                    {
                        "name": "ratings.csv",
                        "sha256": "ignored999",
                    },
                ],
            },
            {
                "source_name": "IMDb",
                "files": [
                    {
                        "name": "title.basics.tsv.gz",
                        "sha256": "ccc333",
                    },
                    {
                        "name": "title.ratings.tsv.gz",
                        "sha256": "ddd444",
                    },
                ],
            },
        ]
    }


# ============================================================
# STEP 7 — JOIN SEMANTICS
# ============================================================

def test_join_movies_links_keeps_all_movies(
    movies_df,
    links_df,
):
    result = bmr.join_movies_links(
        movies_df,
        links_df,
    )

    assert len(result) == len(movies_df)
    assert result["movieId"].tolist() == [1, 2, 3]

    movie_3 = result.loc[
        result["movieId"] == 3
    ].iloc[0]

    assert pd.isna(
        movie_3["imdbId"]
    )

    assert pd.isna(
        movie_3["tmdbId"]
    )


def test_join_movies_links_rejects_duplicate_right_keys(
    movies_df,
):
    duplicate_links = pd.DataFrame(
        {
            "movieId": [1, 1],
            "imdbId": [
                "tt0000001",
                "tt9999999",
            ],
            "tmdbId": pd.Series(
                [10, 11],
                dtype="Int64",
            ),
        }
    )

    with pytest.raises(
        pd.errors.MergeError
    ):
        bmr.join_movies_links(
            movies_df,
            duplicate_links,
        )


def test_collect_target_imdb_ids_ignores_null():
    movie_links = pd.DataFrame(
        {
            "imdbId": pd.Series(
                [
                    "tt0000001",
                    "tt0000002",
                    pd.NA,
                    "tt0000001",
                ],
                dtype="string",
            )
        }
    )

    result = (
        bmr.collect_target_imdb_ids(
            movie_links
        )
    )

    assert result == {
        "tt0000001",
        "tt0000002",
    }


def test_join_imdb_basics_keeps_missing_movie(
    movies_df,
    links_df,
    imdb_basics_df,
):
    movie_links = (
        bmr.join_movies_links(
            movies_df,
            links_df,
        )
    )

    result = (
        bmr.join_imdb_basics(
            movie_links,
            imdb_basics_df,
        )
    )

    assert len(result) == 3

    movie_2 = result.loc[
        result["movieId"] == 2
    ].iloc[0]

    movie_3 = result.loc[
        result["movieId"] == 3
    ].iloc[0]

    assert pd.isna(
        movie_2["startYear"]
    )

    assert pd.isna(
        movie_3["startYear"]
    )


def test_join_imdb_ratings_keeps_missing_movie(
    movies_df,
    links_df,
    imdb_basics_df,
    imdb_ratings_df,
):
    movie_links = bmr.join_movies_links(
        movies_df,
        links_df,
    )

    movie_basics = bmr.join_imdb_basics(
        movie_links,
        imdb_basics_df,
    )

    result = bmr.join_imdb_ratings(
        movie_basics,
        imdb_ratings_df,
    )

    assert len(result) == 3

    movie_1 = result.loc[
        result["movieId"] == 1
    ].iloc[0]

    movie_2 = result.loc[
        result["movieId"] == 2
    ].iloc[0]

    assert (
        movie_1["averageRating"]
        == "8.1"
    )

    assert pd.isna(
        movie_2["averageRating"]
    )


def test_join_imdb_basics_rejects_duplicate_imdb_records(
    movies_df,
    links_df,
):
    movie_links = bmr.join_movies_links(
        movies_df,
        links_df,
    )

    duplicate_basics = pd.DataFrame(
        {
            "imdbId": [
                "tt0000001",
                "tt0000001",
            ],
            "titleType": [
                "movie",
                "movie",
            ],
            "primaryTitle": [
                "A",
                "A duplicate",
            ],
            "startYear": [
                "2000",
                "2000",
            ],
        }
    )

    with pytest.raises(
        pd.errors.MergeError
    ):
        bmr.join_imdb_basics(
            movie_links,
            duplicate_basics,
        )


# ============================================================
# STEP 8 — RELEASE YEAR RULES
# ============================================================

def test_parse_year_from_movielens_title():
    assert (
        bmr.parse_year_from_movielens_title(
            "Toy Story (1995)"
        )
        == 1995
    )


def test_parse_year_requires_trailing_parenthesized_year():
    assert (
        bmr.parse_year_from_movielens_title(
            "Movie 1995"
        )
        is None
    )

    assert (
        bmr.parse_year_from_movielens_title(
            "Movie (1995) Extra"
        )
        is None
    )


def test_parse_year_from_null_title_returns_none():
    assert (
        bmr.parse_year_from_movielens_title(
            None
        )
        is None
    )


def test_parse_imdb_start_year():
    assert (
        bmr.parse_imdb_start_year(
            "2000"
        )
        == 2000
    )


def test_parse_invalid_imdb_start_year_returns_none():
    assert (
        bmr.parse_imdb_start_year(
            "20XX"
        )
        is None
    )

    assert (
        bmr.parse_imdb_start_year(
            None
        )
        is None
    )


def test_imdb_year_has_priority_over_title_year():
    result = bmr.derive_release_year(
        imdb_start_year="2000",
        movielens_title="Movie A (1999)",
    )

    assert result == 2000


def test_movielens_title_year_is_fallback():
    result = bmr.derive_release_year(
        imdb_start_year=None,
        movielens_title="Movie A (1999)",
    )

    assert result == 1999


def test_release_year_can_be_null():
    result = bmr.derive_release_year(
        imdb_start_year=None,
        movielens_title="Movie Without Year",
    )

    assert result is None


# ============================================================
# STEP 8 — CANONICAL SCHEMA
# ============================================================

def test_build_canonical_reference_has_exact_columns(
    joined_df,
):
    canonical = (
        bmr.build_canonical_reference(
            joined=joined_df,
            ref_version="TEST",
            generated_at=(
                "2026-10-04T10:00:00Z"
            ),
        )
    )

    assert (
        canonical.columns.tolist()
        == bmr.CANONICAL_COLUMNS
    )


def test_build_canonical_reference_preserves_row_count(
    joined_df,
):
    canonical = (
        bmr.build_canonical_reference(
            joined=joined_df,
            ref_version="TEST",
            generated_at=(
                "2026-10-04T10:00:00Z"
            ),
        )
    )

    assert len(canonical) == len(joined_df)


def test_movielens_title_is_preserved(
    joined_df,
):
    canonical = (
        bmr.build_canonical_reference(
            joined=joined_df,
            ref_version="TEST",
            generated_at=(
                "2026-10-04T10:00:00Z"
            ),
        )
    )

    assert (
        canonical.loc[
            canonical["movieId"] == 1,
            "title",
        ].iloc[0]
        == "Movie A (1999)"
    )


def test_genres_are_preserved(
    joined_df,
):
    canonical = (
        bmr.build_canonical_reference(
            joined=joined_df,
            ref_version="TEST",
            generated_at=(
                "2026-10-04T10:00:00Z"
            ),
        )
    )

    assert (
        canonical.loc[
            canonical["movieId"] == 2,
            "genres",
        ].iloc[0]
        == "Comedy|Romance"
    )

    assert (
        canonical.loc[
            canonical["movieId"] == 3,
            "genres",
        ].iloc[0]
        == "(no genres listed)"
    )


def test_canonical_release_year_precedence(
    joined_df,
):
    canonical = (
        bmr.build_canonical_reference(
            joined=joined_df,
            ref_version="TEST",
            generated_at=(
                "2026-10-04T10:00:00Z"
            ),
        )
    )

    # movieId 1: IMDb 2000 overrides title 1999
    assert (
        canonical.loc[
            canonical["movieId"] == 1,
            "release_year",
        ].iloc[0]
        == 2000
    )

    # movieId 2: IMDb missing, title fallback 2001
    assert (
        canonical.loc[
            canonical["movieId"] == 2,
            "release_year",
        ].iloc[0]
        == 2001
    )

    # movieId 3: no IMDb year and no title year
    assert pd.isna(
        canonical.loc[
            canonical["movieId"] == 3,
            "release_year",
        ].iloc[0]
    )


def test_missing_external_metadata_stays_null(
    joined_df,
):
    canonical = (
        bmr.build_canonical_reference(
            joined=joined_df,
            ref_version="TEST",
            generated_at=(
                "2026-10-04T10:00:00Z"
            ),
        )
    )

    row = canonical.loc[
        canonical["movieId"] == 3
    ].iloc[0]

    assert pd.isna(row["imdbId"])
    assert pd.isna(row["tmdbId"])
    assert pd.isna(row["imdb_rating"])
    assert pd.isna(row["imdb_votes"])


def test_imdb_rating_and_votes_types_are_nullable_numeric(
    joined_df,
):
    canonical = (
        bmr.build_canonical_reference(
            joined=joined_df,
            ref_version="TEST",
            generated_at=(
                "2026-10-04T10:00:00Z"
            ),
        )
    )

    assert str(
        canonical["imdb_rating"].dtype
    ) == "Float64"

    assert str(
        canonical["imdb_votes"].dtype
    ) == "Int64"

    assert str(
        canonical["tmdbId"].dtype
    ) == "Int64"

    assert str(
        canonical["release_year"].dtype
    ) == "Int64"


def test_all_rows_share_same_ref_version_and_generated_at(
    joined_df,
):
    ref_version = "20261004-a1b2c3d4"
    generated_at = "2026-10-04T10:00:00Z"

    canonical = (
        bmr.build_canonical_reference(
            joined=joined_df,
            ref_version=ref_version,
            generated_at=generated_at,
        )
    )

    assert (
        canonical["ref_version"]
        .nunique()
        == 1
    )

    assert (
        canonical["ref_version"]
        .iloc[0]
        == ref_version
    )

    assert (
        canonical["generated_at"]
        .nunique()
        == 1
    )

    assert (
        canonical["generated_at"]
        .iloc[0]
        == generated_at
    )


def test_build_canonical_reference_requires_input_columns():
    incomplete = pd.DataFrame(
        {
            "movieId": [1],
            "title": ["Movie A (1999)"],
        }
    )

    with pytest.raises(
        RuntimeError,
        match="missing columns",
    ):
        bmr.build_canonical_reference(
            joined=incomplete,
            ref_version="TEST",
            generated_at=(
                "2026-10-04T10:00:00Z"
            ),
        )


# ============================================================
# STEP 9 — MANIFEST CHECKSUM LOOKUP
# ============================================================

def test_get_manifest_file_sha256(
    source_manifest,
):
    result = (
        bmr.get_manifest_file_sha256(
            source_manifest,
            "MovieLens",
            "movies.csv",
        )
    )

    assert result == "aaa111"


def test_get_manifest_file_sha256_missing_file_fails(
    source_manifest,
):
    with pytest.raises(
        RuntimeError,
        match="Checksum not found",
    ):
        bmr.get_manifest_file_sha256(
            source_manifest,
            "MovieLens",
            "not-exist.csv",
        )


def test_get_manifest_file_sha256_empty_checksum_fails():
    manifest = {
        "sources": [
            {
                "source_name": "MovieLens",
                "files": [
                    {
                        "name": "movies.csv",
                        "sha256": "",
                    }
                ],
            }
        ]
    }

    with pytest.raises(
        RuntimeError,
        match="Missing SHA256",
    ):
        bmr.get_manifest_file_sha256(
            manifest,
            "MovieLens",
            "movies.csv",
        )


# ============================================================
# STEP 9 — SOURCE HASH / VERSION
# ============================================================

def test_reference_source_hash_is_deterministic(
    source_manifest,
):
    first = (
        bmr.calculate_reference_source_hash(
            source_manifest
        )
    )

    second = (
        bmr.calculate_reference_source_hash(
            source_manifest
        )
    )

    assert first == second


def test_reference_source_hash_ignores_ratings_csv(
    source_manifest,
):
    first = (
        bmr.calculate_reference_source_hash(
            source_manifest
        )
    )

    changed = json.loads(
        json.dumps(
            source_manifest
        )
    )

    for source in changed["sources"]:
        if (
            source["source_name"]
            == "MovieLens"
        ):
            for item in source["files"]:
                if (
                    item["name"]
                    == "ratings.csv"
                ):
                    item["sha256"] = (
                        "completely-different"
                    )

    second = (
        bmr.calculate_reference_source_hash(
            changed
        )
    )

    assert first == second


def test_reference_source_hash_changes_when_reference_source_changes(
    source_manifest,
):
    first = (
        bmr.calculate_reference_source_hash(
            source_manifest
        )
    )

    changed = json.loads(
        json.dumps(
            source_manifest
        )
    )

    for source in changed["sources"]:
        if (
            source["source_name"]
            == "IMDb"
        ):
            for item in source["files"]:
                if (
                    item["name"]
                    == "title.ratings.tsv.gz"
                ):
                    item["sha256"] = (
                        "new-imdb-checksum"
                    )

    second = (
        bmr.calculate_reference_source_hash(
            changed
        )
    )

    assert first != second


def test_reference_source_hash_has_sha256_shape(
    source_manifest,
):
    result = (
        bmr.calculate_reference_source_hash(
            source_manifest
        )
    )

    assert re.fullmatch(
        r"[0-9a-f]{64}",
        result,
    )


def test_ref_version_format():
    result = bmr.build_ref_version(
        source_hash=(
            "a1b2c3d4"
            "e5f67890"
        ),
        generated_at=(
            "2026-10-04T10:00:00Z"
        ),
    )

    assert (
        result
        == "20261004-a1b2c3d4"
    )


def test_ref_version_is_deterministic():
    source_hash = (
        "12345678abcdef"
    )

    generated_at = (
        "2026-10-04T10:00:00Z"
    )

    first = bmr.build_ref_version(
        source_hash,
        generated_at,
    )

    second = bmr.build_ref_version(
        source_hash,
        generated_at,
    )

    assert first == second


# ============================================================
# STEP 9 — ATOMIC TSV OUTPUT
# ============================================================

def test_write_reference_atomic_creates_valid_tsv(
    tmp_path,
    joined_df,
):
    canonical = (
        bmr.build_canonical_reference(
            joined=joined_df,
            ref_version=(
                "20261004-a1b2c3d4"
            ),
            generated_at=(
                "2026-10-04T10:00:00Z"
            ),
        )
    )

    output = (
        tmp_path
        / "reference"
        / "movie_reference.tsv"
    )

    bmr.write_reference_atomic(
        canonical,
        output,
    )

    assert output.is_file()

    reloaded = pd.read_csv(
        output,
        sep="\t",
        dtype=str,
        keep_default_na=True,
    )

    assert (
        reloaded.columns.tolist()
        == bmr.CANONICAL_COLUMNS
    )

    assert len(reloaded) == len(canonical)

    assert not (
        output.parent
        / "movie_reference.tsv.tmp"
    ).exists()


def test_write_reference_atomic_serializes_null_as_empty_field(
    tmp_path,
    joined_df,
):
    canonical = (
        bmr.build_canonical_reference(
            joined=joined_df,
            ref_version="TEST",
            generated_at=(
                "2026-10-04T10:00:00Z"
            ),
        )
    )

    output = (
        tmp_path
        / "movie_reference.tsv"
    )

    bmr.write_reference_atomic(
        canonical,
        output,
    )

    text = output.read_text(
        encoding="utf-8"
    )

    assert "<NA>" not in text
    assert "\tNone\t" not in text
    assert "\tnan\t" not in text.lower()


def test_sha256_file_matches_hashlib(
    tmp_path,
):
    path = (
        tmp_path
        / "sample.txt"
    )

    content = b"movie-reference-test\n"

    path.write_bytes(
        content
    )

    expected = hashlib.sha256(
        content
    ).hexdigest()

    assert (
        bmr.sha256_file(path)
        == expected
    )


# ============================================================
# STEP 9 — ORCHESTRATION SMOKE TEST
# ============================================================

def test_build_and_write_reference_orchestration(
    monkeypatch,
    tmp_path,
    joined_df,
    source_manifest,
):
    """
    Test B9 orchestration without reading real large IMDb files.

    Heavy I/O functions are replaced with deterministic test fixtures.
    """

    output_path = (
        tmp_path
        / "movie_reference.tsv"
    )

    monkeypatch.setattr(
        bmr,
        "REFERENCE_OUTPUT_PATH",
        output_path,
    )

    monkeypatch.setattr(
        bmr,
        "ensure_required_files",
        lambda: None,
    )

    monkeypatch.setattr(
        bmr,
        "utc_now",
        lambda: (
            "2026-10-04T10:00:00Z"
        ),
    )

    monkeypatch.setattr(
        bmr,
        "load_source_manifest",
        lambda: source_manifest,
    )

    monkeypatch.setattr(
        bmr,
        "build_joined_reference",
        lambda: joined_df.copy(),
    )

    monkeypatch.setattr(
        bmr,
        "print_join_summary",
        lambda _: None,
    )

    result = (
        bmr.build_and_write_reference()
    )

    assert output_path.is_file()

    assert result["row_count"] == 3

    assert (
        result["generated_at"]
        == "2026-10-04T10:00:00Z"
    )

    assert re.fullmatch(
        r"20261004-[0-9a-f]{8}",
        result["ref_version"],
    )

    assert re.fullmatch(
        r"[0-9a-f]{64}",
        result["source_hash"],
    )

    assert re.fullmatch(
        r"[0-9a-f]{64}",
        result["sha256"],
    )

    reloaded = pd.read_csv(
        output_path,
        sep="\t",
        dtype=str,
    )

    assert (
        reloaded.columns.tolist()
        == bmr.CANONICAL_COLUMNS
    )

    assert len(reloaded) == 3

    assert (
        reloaded["ref_version"]
        .nunique()
        == 1
    )

    assert (
        reloaded["generated_at"]
        .nunique()
        == 1
    )
