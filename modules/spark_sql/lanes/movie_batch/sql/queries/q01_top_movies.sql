-- Query 1 — Top phim (DATA_CONTRACT_Phase2 mục 8.1; DATA_CONTRACT 13.3).
-- Đúng một kỳ: period_type month/year/all và period_start của kỳ đó (all: NULL). Không trộn nhiều kỳ.
-- Thứ hạng lấy nguyên rank_in_period của analytics_top_movies (interaction_count giảm dần, hòa thì movieId tăng dần).
SELECT rank_in_period,
       movieId,
       title,
       interaction_count,
       rating_count,
       rating_sum,
       avg_rating,
       period_type,
       period_start,
       batch_run_id,
       batch_cutoff_time
FROM analytics_top_movies
WHERE period_type = :period_type
  AND period_start <=> CAST(:period_start AS DATE)
  AND rank_in_period <= :top_n
ORDER BY rank_in_period
