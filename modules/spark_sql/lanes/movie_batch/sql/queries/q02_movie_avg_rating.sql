-- Query 2 — Rating trung bình theo phim (DATA_CONTRACT_Phase2 mục 8.1; DATA_CONTRACT 14.2).
-- avg_rating = rating_sum / rating_count của đúng grain (phim × một kỳ), không AVG(avg_rating) của các kỳ nhỏ hơn.
-- :min_ratings là ngưỡng số rating tối thiểu (mặc định 1 = không lọc). Lọc theo danh sách phim làm ở queries.py.
SELECT movieId,
       title,
       rating_count,
       rating_sum,
       rating_sum / rating_count AS avg_rating,
       period_type,
       period_start,
       batch_run_id,
       batch_cutoff_time
FROM analytics_timeseries
WHERE entity_type = 'movie'
  AND period_type = :period_type
  AND period_start <=> CAST(:period_start AS DATE)
  AND rating_count >= :min_ratings
ORDER BY avg_rating DESC, rating_count DESC, movieId
