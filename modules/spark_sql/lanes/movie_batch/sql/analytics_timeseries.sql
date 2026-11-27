-- analytics_timeseries (DATA_CONTRACT_Phase2 mục 6, 6.1). Grain (batch_run_id, period_type, period_start, entity_type, movieId).
-- entity_type = 'movie': một phim trong một kỳ, dùng cho D2.
-- entity_type = 'all'  : toàn hệ thống trong một kỳ, dùng cho D1; có distinct_movie_count, movieId/title = NULL.
-- avg_rating = rating_sum / rating_count tính trên đúng grain (mục 5), không average-of-averages.
-- :ref_version và :manifest_hash lấy từ manifest của run (source index không chứa hai field này).
SELECT s.batch_run_id,
       s.batch_cutoff_time,
       :ref_version AS ref_version,
       :manifest_hash AS manifest_hash,
       s.period_type,
       s.period_start,
       'movie' AS entity_type,
       s.movieId,
       m.title,
       s.interaction_count,
       s.rating_count,
       s.rating_sum,
       CASE WHEN s.rating_count > 0 THEN s.rating_sum / s.rating_count END AS avg_rating,
       CAST(NULL AS BIGINT) AS distinct_movie_count
FROM movie_period_stats s
LEFT JOIN batch_movie m
       ON m.batch_run_id = s.batch_run_id AND m.movieId = s.movieId
UNION ALL
SELECT batch_run_id,
       batch_cutoff_time,
       :ref_version,
       :manifest_hash,
       period_type,
       period_start,
       'all',
       CAST(NULL AS INT),
       CAST(NULL AS STRING),
       SUM(interaction_count),
       SUM(rating_count),
       SUM(rating_sum),
       CASE WHEN SUM(rating_count) > 0 THEN SUM(rating_sum) / SUM(rating_count) END,
       COUNT(DISTINCT movieId)
FROM movie_period_stats
GROUP BY batch_run_id, batch_cutoff_time, period_type, period_start
