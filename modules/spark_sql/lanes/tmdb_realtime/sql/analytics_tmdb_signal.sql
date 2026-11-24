SELECT
    tmdbId,
    event_time,
    popularity,
    vote_count,
    vote_average,
    prev_popularity,
    popularity_delta,
    updated_at,
    CASE
        WHEN title IS NULL OR TRIM(title) = '' THEN NULL
        ELSE TRIM(title)
    END AS title,
    'tmdb' AS source
FROM tmdb_rt
ORDER BY event_time DESC, popularity DESC, tmdbId ASC
LIMIT 20
