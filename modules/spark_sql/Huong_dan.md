# Hướng dẫn sử dụng `modules/spark_sql` (cho P2, P3, P4)

Tài liệu nhanh cách dùng hạ tầng Spark SQL dùng chung trong `modules/spark_sql/common/` để đọc dữ liệu từ Elasticsearch, chạy Spark SQL chuẩn UTC, sinh `_id` đúng contract và ghi kết quả analytics lên Elasticsearch.

---

## 1. Chuẩn bị nhanh

1. **Chạy từ thư mục gốc repo `movie-trend/`** bằng Python trong `.venv` (kèm `PYTHONPATH=.` khi chạy script trực tiếp):
   ```bash
   PYTHONPATH=. .venv/bin/python <path_to_script.py>
   ```
2. **Bật port-forward Elasticsearch** (nếu test/chạy với ES trên k3s ở terminal khác):
   ```bash
   make -C modules/elasticsearch forward
   ```
   Kiểm tra `.env` có `ES_HTTP_URL=http://127.0.0.1:9200` (nếu bỏ trống thì `ESClient` cũng mặc định dùng `http://127.0.0.1:9200`).
3. **3 quy tắc bắt buộc:**
   - Luôn tạo session bằng `create_spark_sql_session()` (đã khóa `UTC` và tối ưu RAM `768m`).
   - **Không tự tạo `_id`**; dùng `write_analytics_df(...)` (hoặc `serialize_key_v1`) để hệ thống tự băm SHA-256 đúng chuẩn `v1`.
   - Chỉ ghi vào index thuộc `owner` của mình. Khi thử nghiệm, luôn ghi vào prefix `dev_p<N>_*` (không đè lên index canonical/alias).

---

## 2. Tra cứu nhanh

### 2.1. Sáu Temp View đầu vào (`es_reader.py`)

| Tên `temp_view` | Index/Alias mặc định | Các cột chính | Owner nguồn |
|---|---|---|---|
| `batch_movie` | `movie_batch_stats` | `movieId`, `title`, `total_count`, `rating_count`, `rating_sum`, `avg_rating`, `batch_run_id`, `batch_cutoff_time`, `generated_at` | P2 |
| `batch_movie_daily` | `movie_daily_stats` | `date`, `movieId`, `interaction_count`, `rating_count`, `rating_sum`, `avg_rating`, `batch_run_id`, `batch_cutoff_time`, `generated_at` | P2 |
| `batch_genre` | `genre_trends` | `date`, `genre`, `interaction_count`, `rating_count`, `rating_sum`, `avg_rating`, `batch_run_id`, `batch_cutoff_time`, `generated_at` | P3 |
| `rt_movie` | `movie_realtime_stats` | `window_start`, `window_end`, `window_size`, `movieId`, `interaction_count`, `rating_count`, `rating_sum`, `avg_rating`, `updated_at` | P4 |
| `rt_genre` | `genre_realtime` | `window_start`, `window_end`, `window_size`, `genre`, `interaction_count`, `rating_count`, `rating_sum`, `avg_rating`, `updated_at` | P4 |
| `tmdb_rt` | `tmdb_realtime` | `tmdbId`, `event_time`, `popularity`, `vote_count`, `vote_average`, `prev_popularity`, `popularity_delta`, `updated_at` | P5 |

### 2.2. Quyền ghi & bộ cột khóa `_id` (`ANALYTICS_KEY_COLUMNS`)

| Thành viên | `owner` | Pattern index được phép ghi | Bảng analytics | Tuple khóa trong `ANALYTICS_KEY_COLUMNS` |
|---|---|---|---|---|
| **P2** | `"p2"` | `movie_batch_stats*`, `movie_daily_stats*`, `analytics_timeseries*`, `analytics_top_movies*`, `dev_p2_*` | `analytics_timeseries`<br>`analytics_top_movies` | `("batch_run_id", "period_type", "period_start", "entity_type", "movieId")`<br>`("batch_run_id", "period_type", "period_start", "movieId")` |
| **P3** | `"p3"` | `genre_trends*`, `analytics_genre_trends*`, `dev_p3_*` | `analytics_genre_trends` | `("batch_run_id", "period_type", "period_start", "genre")` |
| **P4** | `"p4"` | `analytics_growth*`, `dev_p4_*` | `analytics_growth` | `("window_size", "window_start", "entity_type", "entity_key")` |

---

## 3. Code ví dụ các trường hợp cụ thể

### Trường hợp 1 — P2 (Batch phim): Đọc physical source theo `run_id` → Spark SQL → Ghi physical analytics

> **Lưu ý Batch:** Khi build analytics Batch, P2 **phải truyền `index_overrides` trỏ tới physical index theo `run_id`** (không đọc alias đang publish). Nếu đang dev thử nghiệm, đổi `target_index` thành `dev_p2_analytics_top_movies`.

```python
from modules.spark_sql.common.readers.es_reader import register_all_temp_views
from modules.spark_sql.common.runtime.spark_session import create_spark_sql_session
from modules.spark_sql.common.writers.analytics_writer import (
    ANALYTICS_KEY_COLUMNS,
    write_analytics_df,
)

run_id = "20261122_run01"
spark = create_spark_sql_session(app_name="p2-top-movies")

try:
    # 1. Nạp đúng physical source index của run_id vào Temp Views
    counts = register_all_temp_views(
        spark,
        views=["batch_movie", "batch_movie_daily"],
        index_overrides={
            "batch_movie": f"movie_batch_stats_{run_id}",
            "batch_movie_daily": f"movie_daily_stats_{run_id}",
        },
    )
    print("Source counts:", counts)

    # 2. Chạy Spark SQL tính analytics_top_movies (kỳ all, giữ đủ mọi phim, không LIMIT 10)
    df_top = spark.sql("""
        WITH agg AS (
            SELECT
                d.batch_run_id,
                d.batch_cutoff_time,
                d.movieId,
                SUM(d.interaction_count) AS interaction_count,
                SUM(d.rating_count) AS rating_count,
                SUM(d.rating_sum) AS rating_sum
            FROM batch_movie_daily d
            GROUP BY d.batch_run_id, d.batch_cutoff_time, d.movieId
        )
        SELECT
            a.batch_run_id,
            a.batch_cutoff_time,
            'all' AS period_type,
            CAST(NULL AS DATE) AS period_start,
            a.movieId,
            COALESCE(b.title, CONCAT('MovieLens ID ', CAST(a.movieId AS STRING))) AS title,
            a.interaction_count,
            a.rating_count,
            a.rating_sum,
            CASE
                WHEN a.rating_count > 0 THEN a.rating_sum / a.rating_count
                ELSE CAST(NULL AS DOUBLE)
            END AS avg_rating,
            CAST(
                ROW_NUMBER() OVER (ORDER BY a.interaction_count DESC, a.movieId ASC)
                AS INT
            ) AS rank_in_period
        FROM agg a
        LEFT JOIN batch_movie b ON a.movieId = b.movieId
    """)

    # 3. Ghi physical analytics lên ES (khi test dùng target_index="dev_p2_analytics_top_movies")
    result = write_analytics_df(
        df=df_top,
        target_index=f"analytics_top_movies_{run_id}",
        key_columns=ANALYTICS_KEY_COLUMNS["analytics_top_movies"],
        owner="p2",
        refresh=True,
    )
    print(f"Ghi thành công {result.succeeded_count}/{result.total_sent} docs")
finally:
    spark.stop()
```

---

### Trường hợp 2 — P3 (Batch thể loại): Đọc 1 physical source → Ghi `analytics_genre_trends`

```python
from modules.spark_sql.common.readers.es_reader import register_temp_view
from modules.spark_sql.common.runtime.spark_session import create_spark_sql_session
from modules.spark_sql.common.writers.analytics_writer import (
    ANALYTICS_KEY_COLUMNS,
    write_analytics_df,
)

run_id = "20261122_run01"
spark = create_spark_sql_session(app_name="p3-genre-trends")

try:
    # 1. Đọc 1 physical index của run_id và đăng ký Temp View 'batch_genre'
    register_temp_view(
        spark=spark,
        temp_view="batch_genre",
        index_name=f"genre_trends_{run_id}",
    )

    # 2. Tổng hợp theo tháng UTC (weighted average + ROW_NUMBER tie-break theo genre ASC)
    df_genre = spark.sql("""
        WITH agg AS (
            SELECT
                batch_run_id,
                batch_cutoff_time,
                CAST(date_trunc('month', date) AS DATE) AS period_start,
                genre,
                SUM(interaction_count) AS interaction_count,
                SUM(rating_count) AS rating_count,
                SUM(rating_sum) AS rating_sum
            FROM batch_genre
            GROUP BY batch_run_id, batch_cutoff_time, CAST(date_trunc('month', date) AS DATE), genre
        )
        SELECT
            batch_run_id,
            batch_cutoff_time,
            'month' AS period_type,
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
                    PARTITION BY period_start
                    ORDER BY interaction_count DESC, genre ASC
                ) AS INT
            ) AS rank_in_period
        FROM agg
    """)

    # 3. Ghi physical analytics lên ES (khi test dùng target_index="dev_p3_analytics_genre_trends")
    result = write_analytics_df(
        df=df_genre,
        target_index=f"analytics_genre_trends_{run_id}",
        key_columns=ANALYTICS_KEY_COLUMNS["analytics_genre_trends"],
        owner="p3",
        refresh=True,
    )
    print(f"Ghi thành công {result.succeeded_count} docs")
finally:
    spark.stop()
```

---

### Trường hợp 3 — P4 (MovieLens Realtime): Đọc ES có lọc `query`, JOIN lấy `title` → Ghi `analytics_growth`

Ví dụ dưới đây minh họa cách dùng tham số `query` của `register_temp_view` để lọc ngay từ ES (chỉ kéo cửa sổ `1h`), ghép cửa sổ trước đúng `window_start - INTERVAL 1 HOUR`, và chỉ lấy `title` từ `batch_movie` (không lấy số liệu Batch vào metric Realtime).

```python
from modules.spark_sql.common.readers.es_reader import register_temp_view
from modules.spark_sql.common.runtime.spark_session import create_spark_sql_session
from modules.spark_sql.common.writers.analytics_writer import (
    ANALYTICS_KEY_COLUMNS,
    write_analytics_df,
)

spark = create_spark_sql_session(app_name="p4-analytics-growth")

try:
    # 1. Đọc dev index (hoặc bỏ index_name để đọc canonical 'movie_realtime_stats')
    register_temp_view(
        spark=spark,
        temp_view="rt_movie",
        index_name="dev_p4_movie_realtime_stats",
        query={"term": {"window_size": "1h"}},
    )
    # Đọc alias batch_movie đã publish để tra cứu tên phim
    register_temp_view(spark=spark, temp_view="batch_movie")

    # 2. Tính trending_score theo đúng cửa sổ liền kề; thiếu kỳ trước -> null & false
    df_growth = spark.sql("""
        SELECT
            c.window_start,
            c.window_end,
            c.window_size,
            'movie' AS entity_type,
            CAST(c.movieId AS STRING) AS entity_key,
            c.movieId,
            CAST(NULL AS STRING) AS genre,
            b.title,
            CASE
                WHEN b.title IS NOT NULL THEN CONCAT('batch_movie/', b.batch_run_id)
                ELSE CAST(NULL AS STRING)
            END AS title_source,
            c.interaction_count AS current_window_count,
            p.interaction_count AS previous_window_count,
            (p.window_start IS NOT NULL) AS previous_window_present,
            CASE
                WHEN p.window_start IS NOT NULL
                    THEN CAST(c.interaction_count - p.interaction_count AS DOUBLE)
                         / CAST(p.interaction_count + 1 AS DOUBLE)
                ELSE CAST(NULL AS DOUBLE)
            END AS trending_score,
            c.rating_count,
            c.rating_sum,
            c.avg_rating,
            c.updated_at,
            'replay_history' AS timeline_mode
        FROM rt_movie c
        LEFT JOIN rt_movie p
            ON c.movieId = p.movieId
            AND c.window_size = p.window_size
            AND p.window_start = c.window_start - INTERVAL 1 HOUR
        LEFT JOIN batch_movie b
            ON c.movieId = b.movieId
    """)

    # 3. Ghi lên dev index của P4 (production dùng target_index="analytics_growth")
    result = write_analytics_df(
        df=df_growth,
        target_index="dev_p4_analytics_growth",
        key_columns=ANALYTICS_KEY_COLUMNS["analytics_growth"],
        owner="p4",
        refresh=True,
    )
    print(f"Ghi thành công {result.succeeded_count} docs")
finally:
    spark.stop()
```

---

### Trường hợp 4 — Viết Unit Test offline (không cần bật ES)

Dùng `rows_to_dataframe(...)` để nạp dict fixture đúng schema Spark, chạy `spark.sql(...)`, rồi dùng `prepare_analytics_records(...)` để kiểm tra định dạng UTC, `_id` và lỗi trùng khóa mà không cần kết nối ES.

```python
from modules.spark_sql.common.readers.es_reader import rows_to_dataframe
from modules.spark_sql.common.runtime.spark_session import create_spark_sql_session
from modules.spark_sql.common.writers.analytics_writer import (
    ANALYTICS_KEY_COLUMNS,
    prepare_analytics_records,
)


def test_p2_top_movies_offline():
    spark = create_spark_sql_session(app_name="test-p2-offline")
    try:
        fixture_daily = [
            {
                "date": "2017-05-03",
                "movieId": 356,
                "interaction_count": 10,
                "rating_count": 10,
                "rating_sum": 45.0,
                "avg_rating": 4.5,
                "batch_run_id": "run_test",
                "batch_cutoff_time": "2017-05-04T00:00:00Z",
                "generated_at": "2017-05-04T00:05:00Z",
            }
        ]
        rows_to_dataframe(spark, "batch_movie_daily", fixture_daily).createOrReplaceTempView(
            "batch_movie_daily"
        )

        df = spark.sql("""
            SELECT
                batch_run_id,
                'all' AS period_type,
                CAST(NULL AS DATE) AS period_start,
                movieId,
                SUM(interaction_count) AS interaction_count
            FROM batch_movie_daily
            GROUP BY batch_run_id, movieId
        """)

        records = prepare_analytics_records(
            df, key_columns=ANALYTICS_KEY_COLUMNS["analytics_top_movies"]
        )
        assert len(records) == 1
        assert len(records[0]["_id"]) == 64  # SHA-256 hex
        assert records[0]["period_start"] is None
    finally:
        spark.stop()
```

---

### Trường hợp 5 — Gọi trực tiếp `serialize_key_v1` / `build_record_id` (khi viết loader hoặc đối chiếu `_id`)

Dùng khi P2/P3/P4 tự nạp source index lên ES bằng `ESClient.bulk_upsert` hoặc muốn kiểm tra 2 cách tạo khóa cho ra cùng 1 `_id`:

```python
from datetime import date, datetime, timezone
from modules.spark_sql.common.writers.key_serializer import (
    KEY_SERIALIZER_VERSION,
    build_record_id,
    serialize_key_v1,
)

assert KEY_SERIALIZER_VERSION == "v1"

# 1. Tạo _id từ tuple theo đúng thứ tự
doc_id = serialize_key_v1(("run_01", "day", date(2017, 5, 3), "movie", 356))

# 2. Tạo _id từ dict record + danh sách cột khóa (tương đương với cách trên)
record = {
    "batch_run_id": "run_01",
    "period_type": "day",
    "period_start": date(2017, 5, 3),
    "entity_type": "movie",
    "movieId": 356,
}
assert build_record_id(record, ("batch_run_id", "period_type", "period_start", "entity_type", "movieId")) == doc_id

# 3. Các quy tắc phân biệt kiểu:
assert serialize_key_v1((1,)) != serialize_key_v1(("1",))      # int != str
assert serialize_key_v1((None,)) != serialize_key_v1(("null",)) # None != "null"

# 4. Timestamp BẮT BUỘC phải có tzinfo (tránh lỗi lệch +07:00 trên máy VN):
ts_id = serialize_key_v1(("1h", datetime(2017, 5, 3, 21, 0, tzinfo=timezone.utc), "movie", "356"))
# Chú ý: datetime(2017, 5, 3, 21, 0) không có tzinfo sẽ ném ValueError ngay!
```

---

## 4. Lỗi thường gặp & lệnh kiểm tra nhanh

| Lỗi gặp phải | Nguyên nhân | Cách sửa |
|---|---|---|
| `PermissionError` khi gọi `write_analytics_df` | `owner` không khớp với `target_index` | Kiểm tra lại `owner="p2"/"p3"/"p4"` và tên index đích (nên dùng `dev_p<N>_*` khi test). |
| `ValueError: Phát hiện trùng khóa _id=...` | Câu SQL trả ra >= 2 dòng có cùng tuple khóa | Kiểm tra lại `GROUP BY`, bộ lọc `period_type`/`window_size` hoặc `ANALYTICS_KEY_COLUMNS`. |
| `ValueError: Timestamp ... BẮT BUỘC phải có tzinfo` | Truyền `datetime` naive (không có múi giờ) vào reader hoặc serializer | Gắn `tzinfo=timezone.utc` trong Python hoặc truyền chuỗi ISO có hậu tố `Z`. |
| `ValueError: Cột Date không được có giờ khác 00:00:00Z` | Trường `date` / `period_start` chứa timestamp không phải 00:00:00Z | Ép về `date(...)` hoặc `YYYY-MM-DD` (trong SQL dùng `CAST(... AS DATE)`). |
| `BulkOperationError` | Schema/kiểu dữ liệu đầu ra không khớp mapping ES (`dynamic: strict`) | Kiểm tra đúng tên cột và kiểu (`INT`/`LONG`/`DOUBLE`/`DATE`/`TIMESTAMP`) theo `DATA_CONTRACT_Phase2.md`. |

Chạy bộ test kiểm tra hạ tầng chung `modules/spark_sql/common`:

```bash
PYTHONPATH=. .venv/bin/pytest modules/spark_sql/tests/common/ -v
```
