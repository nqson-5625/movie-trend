# Lane `movie_batch` (P2) — schema và fixture

| | |
| --- | --- |
| Contract | `DATA_CONTRACT_Phase2.md` bản **Phase2-3.0** (mục 4, 5, 6.1, 9); `schema_version = phase2-3.0` |
| Owner | P2 — Đỗ Sơn |
| Consumer | P1 (D1: `analytics_timeseries` dòng `entity_type=all`, `analytics_top_movies`), P2 (D2: `analytics_timeseries` dòng `entity_type=movie`) |
| Fixture `generated_at` | `2026-11-24T00:00:00Z` |
| Nhãn | **FIXTURE** — dữ liệu dựng tay để phát triển song song, **không phải** kết quả MapReduce thật. `batch_run_id = fixture-p2-v1` |

Index template tương ứng nằm ở `modules/elasticsearch/lanes/movie_batch/templates/`.

## Bốn bảng của lane

| Bảng | Loại | Grain / `_id` (`serialize_key_v1`) | Nguồn |
| --- | --- | --- | --- |
| `movie_batch_stats` | source | `(batch_run_id, movieId)` | `movie_stats.tsv` của Module 04 |
| `movie_daily_stats` | source | `(batch_run_id, date, movieId)` | `movie_daily_stats.tsv` của Module 04 (không có `title`) |
| `analytics_timeseries` | analytics | `(batch_run_id, period_type, period_start, entity_type, movieId)` | Spark SQL từ hai source |
| `analytics_top_movies` | analytics | `(batch_run_id, period_type, period_start, movieId)` | Spark SQL từ hai source |

- `analytics_timeseries`: `period_type` ∈ day/month/year/all × `entity_type` ∈ movie/all. Kỳ `all` có
  `period_start = null`; dòng `entity_type=all` có `movieId = title = null` và `distinct_movie_count`;
  dòng `entity_type=movie` có `distinct_movie_count = null`.
- `analytics_top_movies`: `period_type` ∈ month/year/all, lưu **đủ mọi phim** trong kỳ;
  `rank_in_period` = row_number theo `interaction_count` giảm dần, hòa thì `movieId` tăng dần. Panel tự lọc `<= 10`.
- Mọi kỳ: `avg_rating = rating_sum / rating_count` tính trên đúng grain (không average-of-averages).
- Khi tạo `_id`, `date`/`period_start` phải là kiểu ngày (không phải chuỗi) để khớp writer Spark dùng chung.

## Thư mục

```text
contracts/phase2/movie_batch/
├── README.md
├── build_fixtures.py        sinh fixtures/ từ bảng ngày viết tay (nguồn sự thật duy nhất)
├── schemas/                 JSON Schema Draft 2020-12 cho từng bảng (additionalProperties=false)
└── fixtures/
    ├── manifest.json        manifest giả lập; manifest_hash = sha256 của file này
    ├── movie_batch_stats.jsonl      4 dòng
    ├── movie_daily_stats.jsonl      9 dòng
    ├── analytics_timeseries.jsonl   36 dòng (27 movie + 9 all)
    └── analytics_top_movies.jsonl   18 dòng
```

## Fixture phủ những ca nào (contract mục 9, P2)

Dữ liệu gốc: 4 phim (1, 2, 3 và 99 không có title), 4 ngày, **12 rating, tổng điểm 44,0**, mốc T = `2018-03-01T00:00:00Z`.

| Ca bắt buộc | Có trong fixture |
| --- | --- |
| Dòng all có `period_start = null` | 1 dòng `period_type=all, entity_type=all`: 12 rating, 4 phim, điểm TB 44/12 |
| day / month / year | 4 ngày, 2 tháng (2017-12, 2018-01), 2 năm (2017, 2018) |
| movie / all | mọi kỳ có cả dòng từng phim và dòng toàn hệ thống |
| Top hai kỳ và ca hòa | Top theo tháng và năm có 2 kỳ; kỳ all: phim 1 và 2 cùng 4 lượt → phim 1 hạng 1; 2018-01: phim 1, 3, 99 cùng 1 lượt |
| Kỳ trống | tháng 2018-02 (trước T) không có rating nào, nên không có dòng; dashboard hiển thị 0, điểm TB để trống |
| Thiếu title | phim 99 có `title = null` |

## Lệnh

```bash
.venv/bin/python contracts/phase2/movie_batch/build_fixtures.py           # sinh lại fixtures/
.venv/bin/python contracts/phase2/movie_batch/build_fixtures.py --check   # kiểm fixtures/ khớp script
.venv/bin/python -m pytest modules/elasticsearch/tests/lanes -q           # template + schema + fixture
```

### Nạp fixture vào Elasticsearch local (để dựng/thử D1, D2)

Cần ES đang chạy theo `modules/elasticsearch/Huong_dan.md` (Secret + `.env` có `ES_*`, `make up`, `make forward`).

```bash
PYTHONPATH=. make -C modules/elasticsearch init-templates   # cài template (gồm 4 template của lane này)
.venv/bin/python -m modules.elasticsearch.lanes.movie_batch.load_fixtures --replace
```

Kết quả: `dev_p2_movie_batch_stats` 4, `dev_p2_movie_daily_stats` 9, `dev_p2_analytics_timeseries` 36,
`dev_p2_analytics_top_movies` 18 document. Loader từ chối ghi nếu template chưa được cài; chạy lại không nhân đôi
(ghi đè theo `_id`). Fixture chỉ nằm trong namespace dev, không gắn vào alias chính (contract mục 4, 9).

Truy vấn mẫu đúng cách D1/D2 đọc:

| Dùng cho | Index | Lọc |
| --- | --- | --- |
| D1 · KPI toàn thời gian | `dev_p2_analytics_timeseries` | `entity_type=all` và `period_type=all` (đúng 1 dòng, `period_start=null`) |
| D1 · Timeline | `dev_p2_analytics_timeseries` | `entity_type=all` và đúng một `period_type` (vd. `month`) |
| D1 · Top 10 | `dev_p2_analytics_top_movies` | đúng một `period_type` (+ một `period_start` nếu month/year) và `rank_in_period <= 10` |
| D2 · Xu hướng 1–3 phim | `dev_p2_analytics_timeseries` | `entity_type=movie`, đúng một `period_type`, `movieId` ∈ 1–3 phim |

## Nạp dữ liệu thật vào source (K1)

Đầu vào là thư mục export của run (`make -C modules/mapreduce download`), mặc định
`data/local/mapreduce/export/run_id=20261024T104145Z/`.

```bash
.venv/bin/python -m modules.elasticsearch.lanes.movie_batch.load_batch_run --dry-run   # chỉ đọc + kiểm tra
.venv/bin/python -m modules.elasticsearch.lanes.movie_batch.load_batch_run             # ghi physical index của run
```

- Ghi `movie_batch_stats_20261024t104145z` và `movie_daily_stats_20261024t104145z`. Không đụng alias:
  alias chính do P5 publish cùng 6 alias Batch sau READY.
- Trước khi ghi, lệnh kiểm tra:
  - đủ cột theo thứ tự trong manifest;
  - số dòng bằng `Reduce output records` trong manifest;
  - từng dòng đúng schema; cùng run, T, `generated_at` với manifest; `avg = sum/count`;
  - cộng theo ngày của từng phim bằng `movie_batch_stats`; không có ngày nào không trước T.
- Sau khi ghi, lệnh hỏi lại ES số dòng và tổng lượt.
- Chạy lại không nhân đôi (ghi đè theo `_id`).
- Báo cáo nạp ghi vào `data/local/phase2/es_reports/p2_source_<run_id>.json` (số dòng, tổng lượt, `ref_version`,
  `manifest_sha256`), dùng khi lập READY report.

## Build analytics từ source (K2)

SQL nằm ở `modules/spark_sql/lanes/movie_batch/sql/`. Job là `modules/spark_sql/lanes/movie_batch/job.py`:
đọc hai physical source của run, chạy SQL, đối soát trong Spark rồi ghi `analytics_timeseries_<run>` và
`analytics_top_movies_<run>`. Job không đụng alias.

Spark 3.5.1 cần Python 3.12, giống CI; Python 3.14 lỗi `RecursionError` trong cloudpickle. Trên máy dev:

```bash
export PYSPARK_PYTHON=~/.venvs/movie-trend-spark/bin/python PYSPARK_DRIVER_PYTHON=$PYSPARK_PYTHON
$PYSPARK_PYTHON -m pytest modules/spark_sql/tests/lanes -q                                  # SQL tái tạo đúng fixture
$PYSPARK_PYTHON -m modules.spark_sql.lanes.movie_batch.job --io-mode fixture --dry-run      # tính + đối soát
$PYSPARK_PYTHON -m modules.spark_sql.lanes.movie_batch.job --io-mode fixture                # ghi physical analytics
```

- `ref_version` và `manifest_hash` lấy từ báo cáo nạp source `p2_source_<run>.json`, tức từ manifest của run.
- Trước khi ghi, job đối soát:
  - có đúng 1 dòng `all/all`, khớp tổng lượt và số phim của source;
  - mỗi `period_type`, cộng các dòng movie bằng tổng source;
  - dòng `all` của từng kỳ bằng tổng các dòng movie kỳ đó;
  - Top có đủ mọi phim của kỳ.
- Báo cáo ghi analytics: `data/local/phase2/es_reports/p2_analytics_<run>.json`.
- `--io-mode distributed` (Spark ES connector) cần `SPARK_ES_PACKAGE`. Hiện `spark_session.py` dùng chung truyền
  `es.nodes` / `es.net.http.auth.*` mà không có tiền tố `spark.`, nên Spark bỏ qua (cảnh báo
  `Ignoring non-Spark config property`). Vì vậy run hiện tại ghi bằng `fixture` (REST bulk qua writer dùng chung):
  hết khoảng 1 phút, bộ nhớ đỉnh khoảng 500 MB.

## Truy vấn bắt buộc của P2: query 1, 2, 4, 5, 6 (contract mục 8.1)

SQL nằm ở `modules/spark_sql/lanes/movie_batch/sql/queries/`, hàm Python ở `modules/spark_sql/lanes/movie_batch/queries.py`.
Truy vấn chạy trên source của run và hai analytics tạo bằng đúng SQL của job, nên luôn khớp D1/D2. Kết quả không ghi
thành index (contract chỉ bắt materialize sáu analytics family).

| # | Truy vấn | Cách tính |
| --- | --- | --- |
| 1 | Top phim | `analytics_top_movies` của **đúng một** kỳ month/year/all, theo `rank_in_period` (hòa thì `movieId` tăng) |
| 2 | Rating trung bình phim | Dòng `entity_type=movie` của một kỳ; `rating_sum / rating_count`, không average-of-averages; ngưỡng `min_ratings` tùy chọn (mặc định 1) |
| 4 | Xu hướng 7 ngày | previous `[mốc-14, mốc-7)`, current `[mốc-7, mốc)`; `growth = (current - previous) / (previous + 1)` (DATA_CONTRACT 14.4) |
| 5 | Xu hướng 30 ngày | Như query 4 với 30 ngày liên tiếp; không thay bằng tháng lịch |
| 6 | WoW | Hai tuần ISO trọn vẹn (thứ Hai 00:00Z): tuần trọn gần nhất kết thúc trước hoặc đúng mốc, so với tuần liền trước |

Quy ước cho query 4–6:
- **Mốc** mặc định là ngày của T. Mốc lấy trên timeline dữ liệu nguồn, không dùng ngày hiện tại của máy (DATA_CONTRACT mục 5.4).
  Muốn demo một giai đoạn có nhiều rating thì truyền `--as-of`.
- **`coverage_status`:** `verified` khi cả hai khoảng nằm trong vùng Batch đã tính (từ ngày có rating đầu tiên tới
  `current_end <= T`). Ngược lại là `unverified`, và số đếm cùng `growth` để NULL.
- `window_bounds()` luôn trả đúng 1 dòng (hai khoảng + `coverage_status`), kể cả khi hai khoảng không có rating nào.

```bash
export PYSPARK_PYTHON=~/.venvs/movie-trend-spark/bin/python PYSPARK_DRIVER_PYTHON=$PYSPARK_PYTHON
$PYSPARK_PYTHON -m modules.spark_sql.lanes.movie_batch.queries q01 --period-type month --period-start 2018-05-01
$PYSPARK_PYTHON -m modules.spark_sql.lanes.movie_batch.queries q02 --movie-id 1 --movie-id 296
$PYSPARK_PYTHON -m modules.spark_sql.lanes.movie_batch.queries q04 --as-of 2018-09-01   # q05, q06 tương tự
```

Kết quả với run thật:
- Query 1 all-time: Forrest Gump 329 · Shawshank 317 · Pulp Fiction 307.
- Query 2: Shawshank 4,429 (317 rating).
- Query 4 với mốc T: previous `[2018-09-17, 2018-09-24)`, current `[2018-09-24, 2018-10-01)`, `verified`, 186 phim.
  Rating cuối là 2018-09-24, nên current chỉ có ngày đó.

Dữ liệu nhỏ (mỗi phim vài rating mỗi tuần) nên growth thường là 0→1 hoặc 0→2. Contract cho phép thêm ngưỡng loại phim ít
tương tác (DATA_CONTRACT 14.3), nhưng ngưỡng đó phải được ghi rõ như một cấu hình; hiện chưa đặt.

### Fixture query 8 "Batch vs Realtime" (P4 làm chủ, P2 cấp phía Batch)

`fixtures/query8/` là phía Batch, với **T = `2018-09-20T11:00:00Z`** nằm giữa hai nguồn:
- Phía Batch: 7 rating trước 11:00 của chính fixture `contracts/phase2/movielens_realtime` (P4).
- Phía Realtime: các cửa sổ `window_start >= T` của fixture P4. Dùng nguyên fixture P4, không phải sửa.
- T tròn giờ nên không cửa sổ 10m/1h nào bị cắt đôi.

| movieId | Batch (< T) | RT (>= T) | Ghép đúng |
| --- | ---: | ---: | ---: |
| 218 | 1 | 4 | 5 |
| 222 | 3 | 5 | 8 |
| 227 | 1 | 2 | 3 |
| 343 | 2 | 3 | 5 |
| 999001 | 0 | 2 | 2 |
| **Tổng** | 7 | 16 | **23** = đủ 23 rating của fixture P4 |

Cửa sổ 10m và 1h đều phải ra 23. Cộng thẳng Batch + mọi cửa sổ RT không lọc T ra 30, tức đếm trùng 7.
Test `test_query8_batch_side_matches_p4_realtime_fixture` kiểm chéo hai fixture. T này chỉ dùng cho fixture;
T của run thật vẫn là `2018-10-01T00:00:00Z`.

## READY report (P2.3)

```bash
.venv/bin/python -m modules.elasticsearch.lanes.movie_batch.emit_ready_report --sql-test-result PASS
```

Lệnh gộp `p2_source_<run>.json` và `p2_analytics_<run>.json`, rồi **kiểm tra lại trực tiếp trên ES** trước khi ghi
`status = READY` vào `data/local/phase2/es_reports/p2_ready_<run>.json`:
- 4 physical index tồn tại, đúng số document;
- mỗi index đúng một `batch_run_id` và một `batch_cutoff_time`;
- hai analytics có đúng một `ref_version` và một `manifest_hash`, trùng manifest của run;
- tổng lượt all-time của cả 4 index bằng nhau (`sum_count`).

Chỉ truyền `--sql-test-result PASS` sau khi `modules/spark_sql/tests/lanes` pass; `FAIL` thì report là `NOT_READY`.
Report dùng `ReadyReport` dùng chung, nên P5 ghép được với `p3_ready_<run>.json` qua `validate_ready_reports_pair`
rồi chạy `make -C modules/elasticsearch publish-dry` / `publish RUN_ID=<run>`.

Run `20261024T104145Z` đã READY:

| Alias | Physical index | Document | Tổng lượt |
| --- | --- | ---: | ---: |
| `movie_batch_stats` | `movie_batch_stats_20261024t104145z` | 9.724 | 100.836 |
| `movie_daily_stats` | `movie_daily_stats_20261024t104145z` | 99.538 | 100.836 |
| `analytics_timeseries` | `analytics_timeseries_20261024t104145z` | 237.096 | 100.836 |
| `analytics_top_movies` | `analytics_top_movies_20261024t104145z` | 133.157 | 100.836 |

Metadata để đối chiếu với P3 (phải trùng): `batch_run_id = 20261024T104145Z`,
`batch_cutoff_time = 2018-10-01T00:00:00Z`, `ref_version = 20261017-5f26995f`,
`manifest_sha256 = ad81995983a5dd5d82994f41928864b66ab1eeb027a0b53af30c00b6221ca635`.

## Số đối chứng với dữ liệu thật (run `20261024T104145Z`, T = `2018-10-01T00:00:00Z`)

| Bảng | Số dòng kỳ vọng |
| --- | --- |
| `movie_batch_stats` | 9.724 |
| `movie_daily_stats` | 99.538 |
| `analytics_timeseries` | 237.096 (movie: 99.538 day + 82.138 month + 41.295 year + 9.724 all; all: 4.110 day + 267 month + 23 year + 1 all) |
| `analytics_top_movies` | 133.157 (82.138 month + 41.295 year + 9.724 all) |

Dòng all/all thật: 100.836 rating, 9.724 phim, điểm TB 353.083 / 100.836 ≈ 3,50.
