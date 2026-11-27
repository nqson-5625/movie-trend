-- Query 3 (DATA_CONTRACT_Phase2.md mục 8.1):
-- Thống kê genre theo thời gian ở các mức day / month / year và tuần ISO (Monday 00:00Z UTC).
-- Giữ đa nhãn (multi-label) và UNKNOWN; dùng weighted average rating_sum / rating_count.

WITH iso_week AS (
    SELECT
        batch_run_id,
        batch_cutoff_time,
        'iso_week' AS period_type,
        CAST(date_trunc('week', date) AS DATE) AS period_start,
        date_format(date, "YYYY-'W'ww") AS iso_week_label,
        genre,
        SUM(interaction_count) AS interaction_count,
        SUM(rating_count) AS rating_count,
        SUM(rating_sum) AS rating_sum
    FROM batch_genre
    GROUP BY
        batch_run_id,
        batch_cutoff_time,
        CAST(date_trunc('week', date) AS DATE),
        date_format(date, "YYYY-'W'ww"),
        genre
)
SELECT
    batch_run_id,
    batch_cutoff_time,
    period_type,
    period_start,
    iso_week_label,
    genre,
    interaction_count,
    rating_count,
    rating_sum,
    CASE
        WHEN rating_count > 0 THEN rating_sum / rating_count
        ELSE CAST(NULL AS DOUBLE)
    END AS avg_rating,
    CAST(
        ROW_NUMBER() OVER (
            PARTITION BY batch_run_id, period_type, period_start
            ORDER BY interaction_count DESC, genre ASC
        ) AS INT
    ) AS rank_in_period
FROM iso_week
ORDER BY period_start ASC, rank_in_period ASC
