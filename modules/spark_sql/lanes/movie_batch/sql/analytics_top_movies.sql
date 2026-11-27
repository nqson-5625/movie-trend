-- analytics_top_movies (DATA_CONTRACT_Phase2 mục 5, 6, 6.1). Grain (batch_run_id, period_type, period_start, movieId).
-- Kỳ month / year / all; lưu đủ mọi phim có rating trong kỳ, panel D1 tự lọc rank_in_period <= 10.
-- rank_in_period = row_number theo interaction_count giảm dần, hòa thì movieId tăng dần (rank ổn định giữa các lần chạy).
SELECT s.batch_run_id,
       s.batch_cutoff_time,
       :ref_version AS ref_version,
       :manifest_hash AS manifest_hash,
       s.period_type,
       s.period_start,
       s.movieId,
       m.title,
       s.interaction_count,
       s.rating_count,
       s.rating_sum,
       CASE WHEN s.rating_count > 0 THEN s.rating_sum / s.rating_count END AS avg_rating,
       CAST(ROW_NUMBER() OVER (
           PARTITION BY s.batch_run_id, s.period_type, s.period_start
           ORDER BY s.interaction_count DESC, s.movieId ASC
       ) AS INT) AS rank_in_period
FROM movie_period_stats s
LEFT JOIN batch_movie m
       ON m.batch_run_id = s.batch_run_id AND m.movieId = s.movieId
WHERE s.period_type IN ('month', 'year', 'all')
