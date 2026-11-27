-- Analytics P3: analytics_genre_trends (day, month, year) từ temp view batch_genre
-- Tuân thủ DATA_CONTRACT_Phase2.md (Phase2-3.0, mục 5 & 6.1):
--   - Weighted average: SUM(rating_sum) / SUM(rating_count), tuyệt đối không AVG(avg_rating).
--   - Thứ hạng rank_in_period: ROW_NUMBER theo interaction_count giảm dần, hòa thì genre tăng dần.
--   - Giữ nguyên thể loại UNKNOWN và tính độc lập từng genre (multi-label).

WITH by_period AS (
    SELECT
        batch_run_id,
        batch_cutoff_time,
        'day' AS period_type,
        date AS period_start,
        genre,
        SUM(interaction_count) AS interaction_count,
        SUM(rating_count) AS rating_count,
        SUM(rating_sum) AS rating_sum
    FROM batch_genre
    GROUP BY batch_run_id, batch_cutoff_time, date, genre

    UNION ALL

    SELECT
        batch_run_id,
        batch_cutoff_time,
        'month' AS period_type,
        CAST(date_trunc('month', date) AS DATE) AS period_start,
        genre,
        SUM(interaction_count) AS interaction_count,
        SUM(rating_count) AS rating_count,
        SUM(rating_sum) AS rating_sum
    FROM batch_genre
    GROUP BY batch_run_id, batch_cutoff_time, CAST(date_trunc('month', date) AS DATE), genre

    UNION ALL

    SELECT
        batch_run_id,
        batch_cutoff_time,
        'year' AS period_type,
        CAST(date_trunc('year', date) AS DATE) AS period_start,
        genre,
        SUM(interaction_count) AS interaction_count,
        SUM(rating_count) AS rating_count,
        SUM(rating_sum) AS rating_sum
    FROM batch_genre
    GROUP BY batch_run_id, batch_cutoff_time, CAST(date_trunc('year', date) AS DATE), genre
)
SELECT
    batch_run_id,
    batch_cutoff_time,
    period_type,
    period_start,
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
FROM by_period
ORDER BY period_type ASC, period_start ASC, rank_in_period ASC
