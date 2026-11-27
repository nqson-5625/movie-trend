-- Query 4 (7 ngày), 5 (30 ngày), 6 (WoW): tăng trưởng theo phim giữa hai khoảng của view window_bounds
-- (DATA_CONTRACT_Phase2 mục 8.1; DATA_CONTRACT 14.4):
--   growth = (current_count - previous_count) / (previous_count + 1)
-- Một dòng cho mỗi phim có rating trong ít nhất một khoảng. Khi coverage_status = 'unverified', số đếm và growth là NULL.
WITH counts AS (
    SELECT d.movieId,
           SUM(CASE WHEN d.date >= b.current_start THEN d.interaction_count ELSE 0 END) AS current_count,
           SUM(CASE WHEN d.date < b.current_start THEN d.interaction_count ELSE 0 END) AS previous_count
    FROM batch_movie_daily d
    CROSS JOIN window_bounds b
    WHERE d.date >= b.previous_start AND d.date < b.current_end
    GROUP BY d.movieId
)
SELECT c.movieId,
       m.title,
       b.window_days,
       b.previous_start,
       b.current_start,
       b.current_end,
       CASE WHEN b.coverage_status = 'verified' THEN c.previous_count END AS previous_count,
       CASE WHEN b.coverage_status = 'verified' THEN c.current_count END AS current_count,
       CASE WHEN b.coverage_status = 'verified'
            THEN CAST(c.current_count - c.previous_count AS DOUBLE) / (c.previous_count + 1) END AS growth,
       b.coverage_status,
       b.batch_run_id,
       b.batch_cutoff_time
FROM counts c
CROSS JOIN window_bounds b
LEFT JOIN batch_movie m ON m.movieId = c.movieId
ORDER BY growth DESC NULLS LAST, current_count DESC NULLS LAST, c.movieId
