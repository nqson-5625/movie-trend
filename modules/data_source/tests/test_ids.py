import pytest

from modules.data_source.src.normalize_ids import (
    normalize_imdb_id,
    normalize_movie_id,
    normalize_tmdb_id,
)


# ============================================================
# movieId
# ============================================================

def test_movie_id_from_string():
    assert normalize_movie_id("296") == 296


def test_movie_id_from_int():
    assert normalize_movie_id(296) == 296


def test_movie_id_zero_is_invalid():
    with pytest.raises(ValueError):
        normalize_movie_id("0")


def test_movie_id_negative_is_invalid():
    with pytest.raises(ValueError):
        normalize_movie_id("-1")


def test_movie_id_null_is_invalid():
    with pytest.raises(ValueError):
        normalize_movie_id(None)


# ============================================================
# tmdbId
# ============================================================

def test_tmdb_id_from_string():
    assert normalize_tmdb_id("680") == 680


def test_tmdb_id_from_int():
    assert normalize_tmdb_id(680) == 680


def test_tmdb_id_empty_is_none():
    assert normalize_tmdb_id("") is None


def test_tmdb_id_none_is_none():
    assert normalize_tmdb_id(None) is None


def test_tmdb_id_nan_string_is_none():
    assert normalize_tmdb_id("nan") is None


def test_tmdb_id_zero_is_invalid():
    with pytest.raises(ValueError):
        normalize_tmdb_id("0")


# ============================================================
# imdbId
# ============================================================

def test_imdb_id_movielens_format():
    assert (
        normalize_imdb_id("0110912")
        == "tt0110912"
    )


def test_imdb_id_already_canonical():
    assert (
        normalize_imdb_id("tt0110912")
        == "tt0110912"
    )


def test_imdb_id_short_is_zero_padded():
    assert (
        normalize_imdb_id("123")
        == "tt0000123"
    )


def test_imdb_id_long_is_not_truncated():
    assert (
        normalize_imdb_id("12345678")
        == "tt12345678"
    )


def test_imdb_id_none():
    assert normalize_imdb_id(None) is None


def test_imdb_id_empty():
    assert normalize_imdb_id("") is None


def test_imdb_id_invalid_characters():
    with pytest.raises(ValueError):
        normalize_imdb_id("abc123")


def test_imdb_id_invalid_tt_only():
    with pytest.raises(ValueError):
        normalize_imdb_id("tt")


def test_imdb_id_decimal_string_invalid():
    with pytest.raises(ValueError):
        normalize_imdb_id("110912.0")

