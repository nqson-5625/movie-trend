from __future__ import annotations

import math
from typing import Any


# ============================================================
# COMMON
# ============================================================

def is_missing(value: Any) -> bool:
    """
    Return True when an ID value represents a missing value.

    Handles:
    - None
    - float NaN
    - empty string
    - "nan"
    - "null"
    - "none"

    IMPORTANT:
    This function does not modify raw data.
    """

    if value is None:
        return True

    if isinstance(value, float):
        try:
            if math.isnan(value):
                return True
        except TypeError:
            pass

    s = str(value).strip()

    return s.lower() in {
        "",
        "nan",
        "null",
        "none",
    }


# ============================================================
# MOVIELENS movieId
# ============================================================

def normalize_movie_id(value: Any) -> int:
    """
    Normalize MovieLens movieId.

    Contract:
        - integer
        - required
        - > 0

    Raises:
        ValueError for invalid values.
    """

    if is_missing(value):
        raise ValueError(
            "movieId is required"
        )

    s = str(value).strip()

    if not s.isdigit():
        raise ValueError(
            f"Invalid movieId: {value!r}"
        )

    movie_id = int(s)

    if movie_id <= 0:
        raise ValueError(
            f"movieId must be > 0: {value!r}"
        )

    return movie_id


# ============================================================
# TMDB ID
# ============================================================

def normalize_tmdb_id(
    value: Any,
) -> int | None:
    """
    Normalize TMDB ID.

    Contract:
        - nullable integer
        - null remains None
        - NEVER replace missing with 0
        - non-null ID must be > 0
    """

    if is_missing(value):
        return None

    s = str(value).strip()

    if not s.isdigit():
        raise ValueError(
            f"Invalid tmdbId: {value!r}"
        )

    tmdb_id = int(s)

    if tmdb_id <= 0:
        raise ValueError(
            f"tmdbId must be > 0: {value!r}"
        )

    return tmdb_id


# ============================================================
# IMDb ID
# ============================================================

def normalize_imdb_id(
    value: Any,
) -> str | None:
    """
    Normalize IMDb ID into canonical form:

        tt<digits>

    Examples:
        "0110912"   -> "tt0110912"
        "tt0110912" -> "tt0110912"
        "123"       -> "tt0000123"

    Missing value:
        -> None

    Important:
        IDs longer than 7 digits are NOT truncated.
    """

    if is_missing(value):
        return None

    s = str(value).strip()

    if s.startswith("tt"):
        digits = s[2:]
    else:
        digits = s

    if not digits.isdigit():
        raise ValueError(
            f"Invalid IMDb id: {value!r}"
        )

    if len(digits) < 7:
        digits = digits.zfill(7)

    return "tt" + digits
