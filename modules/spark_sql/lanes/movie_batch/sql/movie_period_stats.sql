-- Bảng trung gian của lane movie_batch: cộng movie_daily_stats theo kỳ UTC (DATA_CONTRACT_Phase2 mục 6.1).
-- Một dòng = (run, period_type, period_start, movieId). period_start là đầu kỳ UTC; kỳ all có period_start = NULL.
-- Đọc từ temp view batch_movie_daily (physical source của đúng run, không đọc alias đang publish).
WITH periods AS (
    SELECT batch_run_id, batch_cutoff_time, 'day' AS period_type, date AS period_start,
           movieId, interaction_count, rating_count, rating_sum
    FROM batch_movie_daily
    UNION ALL
    SELECT batch_run_id, batch_cutoff_time, 'month', trunc(date, 'MM'),
           movieId, interaction_count, rating_count, rating_sum
    FROM batch_movie_daily
    UNION ALL
    SELECT batch_run_id, batch_cutoff_time, 'year', trunc(date, 'YEAR'),
           movieId, interaction_count, rating_count, rating_sum
    FROM batch_movie_daily
    UNION ALL
    SELECT batch_run_id, batch_cutoff_time, 'all', CAST(NULL AS DATE),
           movieId, interaction_count, rating_count, rating_sum
    FROM batch_movie_daily
)
SELECT batch_run_id,
       batch_cutoff_time,
       period_type,
       period_start,
       movieId,
       SUM(interaction_count) AS interaction_count,
       SUM(rating_count) AS rating_count,
       SUM(rating_sum) AS rating_sum
FROM periods
GROUP BY batch_run_id, batch_cutoff_time, period_type, period_start, movieId
