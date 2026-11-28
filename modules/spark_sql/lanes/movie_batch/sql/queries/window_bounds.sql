-- Hai khoảng so sánh của query 4 (7 ngày), 5 (30 ngày), 6 (WoW) và trạng thái phủ dữ liệu; luôn đúng một dòng.
-- Khoảng nửa mở, cùng độ dài :days, liền kề, mốc trên timeline dữ liệu nguồn (DATA_CONTRACT_Phase2 mục 8.1):
--   previous = [:current_end - 2*:days, :current_end - :days)      current = [:current_end - :days, :current_end)
-- coverage_status = 'verified' khi cả hai khoảng nằm trong vùng Batch đã tính: từ ngày có rating đầu tiên
-- tới T (current_end <= batch_cutoff_time); ngoài vùng đó 'unverified' và query trả NULL cho số đếm, growth.
WITH bounds AS (
    SELECT CAST(:current_end AS DATE) AS current_end,
           date_sub(CAST(:current_end AS DATE), :days) AS current_start,
           date_sub(CAST(:current_end AS DATE), 2 * :days) AS previous_start,
           MIN(date) AS data_start,
           MAX(batch_cutoff_time) AS batch_cutoff_time,
           MAX(batch_run_id) AS batch_run_id
    FROM batch_movie_daily
)
SELECT :days AS window_days,
       previous_start,
       current_start,
       current_end,
       data_start,
       batch_cutoff_time,
       batch_run_id,
       CASE WHEN previous_start >= data_start AND CAST(current_end AS TIMESTAMP) <= batch_cutoff_time
            THEN 'verified' ELSE 'unverified' END AS coverage_status
FROM bounds
