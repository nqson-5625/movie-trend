# DATA_CONTRACT.md
## Phân tích xu hướng xem phim — Data Contract & Interface Specification

**Phiên bản:** `1.0-draft`  
**Trạng thái:** Draft đã đối chiếu với Module 01; chỉ chuyển thành `1.0-frozen` sau khi toàn nhóm review và chấp thuận  
**Phạm vi:** MVP Lambda Architecture + các mở rộng đã xác định trong Kickoff  
**Kiến trúc bắt buộc:** Kafka → Batch `(HDFS + Hadoop MapReduce)` và Speed `(Spark Structured Streaming)` → Elasticsearch → Spark SQL → Analytics/Dashboard  
**Nguyên tắc:** Interface được chốt trước, implementation có thể thay đổi miễn không phá contract.

**Vị trí chuẩn:** [DATA_CONTRACT.md](DATA_CONTRACT.md) tại thư mục gốc. Các
schema kiểm tra được bằng máy nằm trong [contracts/schemas](contracts/schemas);
hướng dẫn đọc mẫu ở [contracts/README.md](contracts/README.md). Contract mô
tả toàn bộ pipeline dự kiến; hiện repository mới xác nhận phần local của
Module 01 qua Bước 15. Có tài liệu/schema không đồng nghĩa đã có Kafka,
HDFS, Spark hoặc Elasticsearch chạy end-to-end.

**Thứ tự đối chiếu:** contract này định nghĩa interface; schema JSON và
adapter phải triển khai cùng rule. Nếu có khác biệt, sửa và kiểm thử trước
khi bàn giao, không để mỗi consumer tự chọn một cách diễn giải. Việc sửa
draft dưới đây không thay thế xác nhận freeze của producer/consumer.

---

# 1. Mục đích của tài liệu

Tài liệu này là **hợp đồng dữ liệu dùng chung cho toàn nhóm**.

Mọi module phải trả lời được 4 câu:

1. Tôi **nhận dữ liệu gì**?
2. Tôi **được phép giả định gì** về dữ liệu đầu vào?
3. Tôi **phải tạo output gì**, đúng schema nào?
4. Module nào sẽ **consume output** đó?

Mục tiêu là tránh các lỗi integration phổ biến:

- mỗi thành viên tự đặt tên field khác nhau;
- Kafka producer và Spark consumer dùng schema khác nhau;
- MapReduce tạo output mà Elasticsearch loader không đọc được;
- Query Layer truy vấn bảng/index chưa tồn tại;
- Batch và Realtime bị double count;
- `event_time` và `ingest_time` bị dùng lẫn;
- thành viên sau phải sửa output của thành viên trước;
- logic nghiệp vụ bị viết lặp lại ở MapReduce, Spark SQL và Dashboard.

> **Quy tắc dự án:** không module nào được tự thay đổi schema, topic, HDFS path, Elasticsearch index, định nghĩa thời gian hoặc business metric đã freeze trong tài liệu này mà chưa thông qua Change Request.

---

# 2. Phạm vi kiến trúc

## 2.1. Data Flow chính thức

```text
EVENT DATA
┌─────────────────────┐        ┌─────────────────────┐
│ MovieLens ratings   │        │ TMDB trending/API   │
│ replay theo time    │        │ snapshot định kỳ    │
└──────────┬──────────┘        └──────────┬──────────┘
           │                               │
           ▼                               ▼
       Producer                         Producer
           │                               │
           └───────────────┬───────────────┘
                           ▼
                         Kafka
                    ┌──────┴──────┐
                    │             │
                    ▼             ▼
                  HDFS     Spark Structured
                Raw Data      Streaming
                    │             │
                    ▼             ▼
               MapReduce     Realtime View
                    │             │
                    ▼             │
                Batch View        │
                    └──────┬──────┘
                           ▼
                    Elasticsearch
                           │
                           ▼
                       Spark SQL
                           │
                           ▼
                      analytics_*
                           │
                           ▼
                   Streamlit Dashboard
                           │
                           ▼
                    Decision Making
```

Reference data đi theo luồng riêng:

```text
MovieLens movies.csv / links.csv
IMDb metadata
          │
          ▼
Data Preparation / Normalize
          │
          ▼
HDFS /movies/ref
      │         │
      ▼         ▼
 MapReduce   Spark Streaming
```

## 2.2. Nguyên tắc Event Data vs Reference Data

### Event Data

Dữ liệu biểu diễn một sự kiện hoặc snapshot thay đổi theo thời gian.

Bao gồm:

- MovieLens rating replay;
- TMDB trending/popularity snapshot.

**Bắt buộc đi qua Kafka** trước khi vào Batch/Speed.

### Reference Data

Dữ liệu dùng để enrich/join, không phải event stream.

Bao gồm:

- MovieLens `movies.csv`;
- MovieLens `links.csv`;
- IMDb title metadata / ratings dùng làm thuộc tính tham chiếu.

Reference data **không bắt buộc đi Kafka**. Nó được chuẩn hóa rồi nạp vào:

```text
HDFS /movies/ref
```

---

# 3. Nguyên tắc đặt tên

## 3.1. Field naming

Để tương thích với dữ liệu nguồn và tránh thay đổi không cần thiết, Contract v1 giữ các khóa MovieLens/TMDB quen thuộc:

```text
userId
movieId
tmdbId
imdbId
```

Các field do hệ thống tạo dùng `snake_case`:

```text
event_id
schema_version
event_time
ingest_time
window_start
window_end
batch_cutoff_time
generated_at
updated_at
```

Không tạo các biến thể:

```text
movie_id
MovieID
movieID
filmId
```

nếu Contract chưa thay đổi.

## 3.2. Dataset/index naming

- Kafka topic: `kebab-case`
- HDFS folder: `snake_case`
- Elasticsearch index: `snake_case`
- Spark SQL temp view: `snake_case`

Ví dụ:

```text
Kafka:         movie-events
HDFS:          /batch/movie_daily_stats
Elasticsearch: movie_realtime_stats
Spark view:    rt_movie
```

---

# 4. ID Contract

## 4.1. `movieId`

- Kiểu: integer
- Nguồn: MovieLens
- Vai trò: khóa phim chính trong pipeline MovieLens
- Bắt buộc: `> 0`

## 4.2. `tmdbId`

- Kiểu: integer
- Nguồn: MovieLens `links.csv` / TMDB API
- Dùng để nối MovieLens với TMDB
- Bắt buộc `> 0` khi có giá trị.
- Nullable trong `movie_reference`; bắt buộc non-null trong `tmdb-snapshots`.
- Không đổi missing mapping thành `0`; không coi `tmdbId` là `movieId`.

## 4.3. `imdbId`

Contract chuẩn hóa IMDb ID dưới dạng string:

```text
tt0110912
```

Không dùng đồng thời hai dạng:

```text
110912
tt0110912
```

trong dataset canonical.

Module Data Preparation chịu trách nhiệm normalize trước khi ghi `movie_reference`.
ID phải có dạng `^tt[0-9]{7,}$`: thêm số 0 nếu ít hơn 7 chữ số và giữ nguyên
ID dài hơn 7 chữ số. Reference cho phép null khi nguồn không có mapping.

## 4.4. `userId`

- Kiểu: integer/long
- Nguồn: MovieLens
- Chỉ là anonymous ID
- Không được coi là thông tin định danh cá nhân
- Bắt buộc là integer `> 0` trong movie event.

---

# 5. Time Contract — BẮT BUỘC

Đây là contract quan trọng nhất của pipeline.

## 5.1. `event_time`

**Ý nghĩa:** thời điểm sự kiện xảy ra trong nguồn dữ liệu.

MovieLens:

```text
event_time = timestamp gốc của rating
```

TMDB:

```text
event_time = thời điểm snapshot được crawl
```

### Quy tắc

- Mọi phép tính xu hướng theo thời gian phải dựa trên `event_time`.
- Spark Streaming window dùng `event_time`.
- HDFS partition raw ưu tiên theo ngày của `event_time`.
- Batch cutoff được định nghĩa trên `event_time`.

## 5.2. `ingest_time`

**Ý nghĩa:** thời điểm event được producer đưa vào Kafka.

Dùng cho:

- đo latency;
- debug;
- monitoring;
- audit pipeline.

**Không dùng `ingest_time` để tính xu hướng nghiệp vụ.**

Đặc biệt với MovieLens replay:

```text
event_time  = thời điểm rating lịch sử
ingest_time = thời điểm replay hiện tại
```

Nếu dùng `ingest_time` làm cửa sổ 10 phút thì kết quả chỉ phản ánh tốc độ replay, không phải xu hướng phim.

## 5.3. Timezone

Tất cả timestamp trao đổi giữa các module phải ở:

```text
UTC
ISO-8601
```

Ví dụ:

```text
2018-05-03T20:15:30Z
```

Dashboard có thể convert sang timezone hiển thị, nhưng không được thay đổi dữ liệu nguồn.

Trên wire JSONL/Kafka của Contract v1, timestamp là **string** UTC có
độ chính xác đến giây: `YYYY-MM-DDTHH:MM:SSZ`. Kiểu `timestamp` trong bảng
schema là kiểu logical sau khi consumer parse. Không dùng epoch thay cho
field này, không gửi datetime thiếu timezone. Spark đặt session timezone
UTC trước khi parse; record có ngày/giờ không hợp lệ phải bị reject.

Artifact local và fixture của Module 01 dùng thời gian xử lý/tạo mẫu cho
`ingest_time`. Đây là giá trị kiểm tra giao diện, không phải bằng chứng đã
đến Kafka. Module 2 phải gọi lại normalizer với ingest time thực tế ngay
trước khi gửi event; không gửi lại timestamp local như Kafka arrival.

## 5.4. `batch_cutoff_time`

Mỗi Batch View phải công bố:

```text
batch_cutoff_time = T
```

Semantic chính thức:

```text
Batch View    : event_time < T
Realtime View : event_time >= T
```

`T` phải được align về boundary 1 giờ để tránh cửa sổ 10 phút/1 giờ bị cắt giữa chừng.

Ví dụ:

```text
T = 2018-05-04T10:00:00Z
```

Batch xử lý:

```text
event_time < 10:00
```

Realtime được Query Layer sử dụng:

```text
window_start >= 10:00
```

Không được `UNION ALL` toàn bộ Batch + Realtime mà không lọc theo cutoff.

Với MovieLens lịch sử, `T` và khoảng ngày truy vấn phải dựa trên timeline
nguồn đang replay, không mặc định dùng ngày hiện tại của máy. Hai cửa sổ
MVP là tumbling window `[window_start, window_end)`, align UTC, có
`window_size` là `10m` hoặc `1h`. Khi hợp nhất phải chọn **một** window size;
không cộng đồng thời view 10 phút và 1 giờ chứa cùng event. Cửa sổ sliding
là mở rộng riêng và chưa dùng để cộng với Batch View theo contract này.

Event đến muộn có `event_time < T` vẫn được lưu raw và được batch run sau
dedup/tính lại. Consumer phải theo dõi late/drop count và độ trễ cập nhật;
watermark của Spark không chứng minh dữ liệu raw đã đầy đủ. Giá trị
watermark/checkpoint cụ thể do Module 5 cấu hình và công bố khi tích hợp.

---

# 6. Schema Versioning

Mọi event Kafka phải có:

```text
schema_version
```

Contract ban đầu:

```text
schema_version = "1.0"
```

Quy tắc version:

- Patch: sửa documentation, không đổi schema.
- Minor: thêm field optional, với khả năng tương thích consumer đã kiểm thử.
- Major: rename/remove/change type field.

Ba schema Module 01 hiện kiểm tra chính xác v1.0 (`additionalProperties=false`).
Khi có v1.1, producer/consumer phải bổ sung validator/schema theo version và
kiểm tra khả năng tương thích trước khi gửi field mới. Consumer đang dùng
schema v1.0 strict không tự chấp nhận payload v1.1 chỉ vì field mới là optional.

Ví dụ:

```text
1.0 → 1.1
```

được phép nếu chỉ thêm field optional.

```text
1.x → 2.0
```

bắt buộc nếu đổi `movieId` từ integer thành string.

---

# 7. Kafka Contract

## 7.1. Topic `movie-events`

### Owner

- Producer: Module 2 Kafka/Ingestion; reuse adapter của Module 1
- Consumer: HDFS Sink + Spark Streaming
- Schema approval: Architecture Lead/Team Lead

### Kafka key

```text
movieId
```

### Value schema

| Field | Type | Required | Rule |
|---|---|---:|---|
| `schema_version` | string | yes | `"1.0"` |
| `event_id` | string | yes | unique, deterministic |
| `source` | string | yes | `movielens` |
| `userId` | long | yes | `> 0` |
| `movieId` | int | yes | `> 0` |
| `rating` | double | yes | `0.5 <= rating <= 5.0` |
| `event_time` | timestamp | yes | UTC ISO-8601 |
| `ingest_time` | timestamp | yes | UTC ISO-8601 |

### `event_id`

Khuyến nghị deterministic:

```text
sha256(
  "movielens|" +
  userId + "|" +
  movieId + "|" +
  original_timestamp
)
```

Mục tiêu:

- dedup;
- trace;
- retry an toàn.

Module 01 đã chọn công thức trên làm identity v1.0: hash UTF-8, lowercase
hex 64 ký tự; `original_timestamp` là Unix epoch seconds gốc, biểu diễn
integer thập phân. Rating và ingest_time không tham gia identity. Retry
cùng source row phải giữ event_id, không tạo ID ngẫu nhiên mới.

### Example

```json
{
  "schema_version": "1.0",
  "event_id": "62a614d798323bfae660cb63c8c9f458969509d6a687c326daab5cba791952ba",
  "source": "movielens",
  "userId": 429,
  "movieId": 22,
  "rating": 4.0,
  "event_time": "1996-03-29T18:36:55Z",
  "ingest_time": "2026-10-08T11:29:28Z"
}
```

> Ví dụ trên cắt từ [movie-events.sample.jsonl](contracts/samples/movie-events.sample.jsonl).
> Ingest time là thời gian tạo fixture local; producer phải truyền thời gian
> thực tế khi gửi Kafka. Event time và event_id giữ theo nguồn.

---

## 7.2. Topic `tmdb-snapshots`

### Owner

- Producer: Module 2 TMDB scheduler/KafkaProducer; Module 1 cung cấp fetch-once/normalizer
- Consumer: HDFS Sink + Spark Streaming

### Kafka key

```text
tmdbId
```

### Value schema

| Field | Type | Required | Rule |
|---|---|---:|---|
| `schema_version` | string | yes | `"1.0"` |
| `event_id` | string | yes | unique |
| `source` | string | yes | `tmdb` |
| `tmdbId` | int | yes | `> 0` |
| `title` | string | yes | non-empty |
| `popularity` | double | yes | `>= 0` |
| `vote_count` | long | yes | `>= 0` |
| `vote_average` | double | yes | `0 <= x <= 10` |
| `event_time` | timestamp | yes | snapshot/crawl time |
| `ingest_time` | timestamp | yes | Kafka ingest time |

`event_id = sha256("tmdb|tmdbId|event_time")`, hash UTF-8 và lowercase hex
64 ký tự. `event_time` dùng string UTC chuẩn hóa ở mục 5.3 và dùng chung
cho một lần lấy snapshot. Retry cùng snapshot giữ identity; lần fetch mới
với event_time mới tạo identity mới. Mọi numeric field phải hữu hạn;
`vote_count` là integer, không truncate từ số thập phân.

### Example

```json
{
  "schema_version": "1.0",
  "event_id": "1deefa0bcf1982cfb51df835675c6ea3448ab5b5dd8b1d6c25c684a99a5a1d4d",
  "source": "tmdb",
  "tmdbId": 969681,
  "title": "Spider-Man: Brand New Day",
  "popularity": 927.1918,
  "vote_count": 4291,
  "vote_average": 8.299,
  "event_time": "2026-10-08T10:44:04Z",
  "ingest_time": "2026-10-08T10:44:04Z"
}
```

Ví dụ cắt từ [tmdb-snapshots.sample.jsonl](contracts/samples/tmdb-snapshots.sample.jsonl); ingest_time local phải được producer cập nhật khi gửi Kafka.

---

# 8. Reference Data Contract

## 8.1. Canonical dataset: `movie_reference`

Nguồn:

```text
MovieLens movies.csv
MovieLens links.csv
IMDb title.basics
IMDb title.ratings
```

### Canonical schema

| Field | Type | Required | Ý nghĩa |
|---|---|---:|---|
| `movieId` | int | yes | MovieLens key |
| `title` | string | yes | tên phim |
| `release_year` | int | no | năm phát hành |
| `genres` | string | yes | thể loại phân tách bằng pipe; consumer tự split khi cần array |
| `imdbId` | string | no | nullable, canonical `^tt[0-9]{7,}$` |
| `tmdbId` | int | no | nullable; khi non-null phải `> 0` |
| `imdb_rating` | double | no | nullable; hữu hạn, trong `[0,10]` |
| `imdb_votes` | long | no | nullable; integer `>= 0` |
| `ref_version` | string | yes | `YYYYMMDD-<8 ký tự source hash>` |
| `generated_at` | timestamp | yes | UTC |

### Canonical HDFS path

Artifact local: `data/prepared/reference/movie_reference.tsv`. TSV UTF-8 có
header theo đúng thứ tự 10 field trong bảng; các cột đều tồn tại, kể cả
nullable fields. Required `no` ở bảng nghĩa là **giá trị được null**, không
phải được bỏ cột. Ô trống biểu diễn null ở năm/ID/rating/votes; các ô
numeric được parse sang integer/double, không coi chuỗi rỗng là số 0.
Đọc bằng TSV parser có hỗ trợ CSV quoting, không split dòng bằng tab thủ công.

`genres` giữ `(no genres listed)` nếu có trong MovieLens. Title giữ nguyên
năm nguồn. Reference dùng movies.csv làm bảng gốc và LEFT JOIN để bảo toàn
tất cả movieId; số record phải bằng số movie nguồn. Release year ưu tiên
IMDb startYear, rồi fallback năm ở cuối title MovieLens.

`ref_version` dùng ngày UTC của build và 8 ký tự đầu SHA-256 trên checksum
của movies.csv, links.csv và hai file IMDb; thuật toán canonical input nằm
ở `build_movie_reference.calculate_reference_source_hash`. Source hash
đầy đủ được ghi trong manifest. `generated_at` dùng chung toàn bộ build.

Raw source:

```text
/movies/ref/raw/
```

Normalized current version:

```text
/movies/ref/current/movie_reference.tsv
```

Version archive:

```text
/movies/ref/version=<ref_version>/movie_reference.tsv
```

### Consumers

- MapReduce Job 3/4
- Spark Structured Streaming static join
- Batch loader/enrichment nếu cần

### Quy tắc

- Consumer không tự join lại raw IMDb/MovieLens.
- Mọi join downstream dùng `movie_reference`.
- Module Data Preparation là owner duy nhất của canonical reference dataset.
- Publish vào version path, kiểm tra checksum/row count và đọc lại trước
  khi cập nhật `current`. Module 3 cung cấp HDFS endpoint/storage, Module 1
  sở hữu nội dung và thao tác publish reference; không chỉnh version đã publish.

---

# 9. HDFS Contract

## 9.1. Raw Event Storage

### MovieLens

```text
/raw/ratings/dt=YYYY-MM-DD/part-*.jsonl
```

Mỗi line là một JSON object theo schema `movie-events`.

### TMDB

```text
/raw/tmdb/dt=YYYY-MM-DD/part-*.jsonl
```

Mỗi line là một JSON object theo schema `tmdb-snapshots`.

### Quy tắc raw zone

Raw zone là:

```text
append-only
immutable
replayable
```

Không được:

- sửa trực tiếp record;
- overwrite lịch sử chỉ để “cho đúng output”;
- viết dữ liệu aggregate vào `/raw`.

Nếu phát hiện bad record:

```text
/quarantine/<source>/dt=YYYY-MM-DD/
```

và phải ghi lý do lỗi.

---

## 9.2. Reference Zone

```text
/movies/ref/
```

Chỉ Module Data Preparation được publish canonical reference. Module HDFS
quản lý infrastructure/storage, không tự rebuild hoặc sửa nội dung reference.

---

## 9.3. Batch Zone

```text
/batch/
├── movie_stats/
├── movie_daily_stats/
├── genre_daily_stats/
└── _meta/
```

Mỗi batch run có `run_id`.

Ví dụ:

```text
/batch/movie_stats/run_id=20261004T050000Z/
/batch/movie_daily_stats/run_id=20261004T050000Z/
/batch/genre_daily_stats/run_id=20261004T050000Z/
```

Metadata:

```text
/batch/_meta/run_id=<run_id>/manifest.json
```

---

# 10. MapReduce Output Contract

Batch Layer bắt buộc là:

```text
HDFS + Hadoop MapReduce
```

Không thay bằng Spark.

## 10.1. `movie_batch_stats`

### Producer

MapReduce Job 1 + Job 2

### Schema

| Field | Type | Required |
|---|---|---:|
| `movieId` | int | yes |
| `title` | string | no |
| `total_count` | long | yes |
| `rating_count` | long | yes |
| `rating_sum` | double | yes |
| `avg_rating` | double | yes |
| `batch_run_id` | string | yes |
| `batch_cutoff_time` | timestamp | yes |
| `generated_at` | timestamp | yes |

### Invariant

```text
avg_rating = rating_sum / rating_count
```

Nếu `rating_count = 0`, record không được sinh ra.

---

## 10.2. `movie_daily_stats`

Đây là Batch View bắt buộc để phục vụ trend 7 ngày / 30 ngày / WoW.

### Schema

| Field | Type | Required |
|---|---|---:|
| `date` | date | yes |
| `movieId` | int | yes |
| `interaction_count` | long | yes |
| `rating_count` | long | yes |
| `rating_sum` | double | yes |
| `avg_rating` | double | yes |
| `batch_run_id` | string | yes |
| `batch_cutoff_time` | timestamp | yes |
| `generated_at` | timestamp | yes |

Unique logical key:

```text
(date, movieId, batch_run_id)
```

---

## 10.3. `genre_daily_stats`

### Input

```text
/raw/ratings
+
/movies/ref/current/movie_reference.tsv
```

### Mapper contract

Không được chỉ emit:

```text
(genre, date) -> 1
```

nếu Reducer phải tính average rating.

Phải emit tối thiểu:

```text
(genre, date) -> (1, rating)
```

### Schema

| Field | Type | Required |
|---|---|---:|
| `date` | date | yes |
| `genre` | string | yes |
| `interaction_count` | long | yes |
| `rating_count` | long | yes |
| `rating_sum` | double | yes |
| `avg_rating` | double | yes |
| `batch_run_id` | string | yes |
| `batch_cutoff_time` | timestamp | yes |
| `generated_at` | timestamp | yes |

Một phim có nhiều genre được explode thành nhiều record logic.

---

# 11. Spark Structured Streaming Contract

Speed Layer đọc event từ Kafka và ghi Realtime View.

## 11.1. Quy tắc bắt buộc

1. Parse event theo schema version.
2. Dùng `event_time` làm event-time column.
3. Không dùng `ingest_time` cho business window.
4. `movie-events` và `tmdb-snapshots` nên chạy query riêng vì timeline khác nhau.
5. Join genre bằng canonical `/movies/ref`.
6. Realtime writes phải idempotent/upsert được.

---

## 11.2. `movie_realtime_stats`

Window hỗ trợ MVP:

```text
10 minutes
1 hour
```

### Schema

| Field | Type | Required |
|---|---|---:|
| `window_start` | timestamp | yes |
| `window_end` | timestamp | yes |
| `window_size` | string | yes |
| `movieId` | int | yes |
| `interaction_count` | long | yes |
| `rating_count` | long | yes |
| `rating_sum` | double | yes |
| `avg_rating` | double | yes |
| `updated_at` | timestamp | yes |

Deterministic document key:

```text
movieId + window_start + window_size
```

`window_size` dùng `10m` hoặc `1h`, theo quy tắc tumbling/UTC tại mục 5.4.
Speed giữ `interaction_count`, `rating_count`, `rating_sum` để Query Layer
tính weighted average. Trending score thuộc Query Layer, tính từ các
aggregate cùng window size và hai cửa sổ liên tiếp; không lưu công thức
khác trong Spark Streaming hay Dashboard.

---

## 11.3. `genre_realtime`

### Input

```text
movie-events
+
movie_reference
```

### Schema

| Field | Type | Required |
|---|---|---:|
| `window_start` | timestamp | yes |
| `window_end` | timestamp | yes |
| `window_size` | string | yes |
| `genre` | string | yes |
| `interaction_count` | long | yes |
| `rating_count` | long | yes |
| `rating_sum` | double | yes |
| `avg_rating` | double | yes |
| `updated_at` | timestamp | yes |

---

## 11.4. `tmdb_realtime`

### Schema

| Field | Type | Required |
|---|---|---:|
| `tmdbId` | int | yes |
| `event_time` | timestamp | yes |
| `popularity` | double | yes |
| `vote_count` | long | yes |
| `vote_average` | double | yes |
| `prev_popularity` | double | no |
| `popularity_delta` | double | no |
| `updated_at` | timestamp | yes |

Không merge trực tiếp TMDB current popularity với MovieLens historical count như thể chúng cùng một thang thời gian.

MVP lưu current snapshot theo `tmdbId`; upsert document ID là chính tmdbId.
Consumer phải có chiến lược giữ snapshot mới nhất theo event_time để retry
hoặc record đến sai thứ tự không ghi đè tín hiệu mới bằng tín hiệu cũ.
`prev_popularity` là snapshot có event_time liền trước theo cùng tmdbId,
`popularity_delta = popularity - prev_popularity`; hai field được null nếu
chưa có snapshot trước. Chiến lược state/checkpoint là trách nhiệm Module 5.

Dashboard có thể hiển thị chúng **song song** hoặc dùng để enrich/contextualize.

---

# 12. Elasticsearch Contract

Elasticsearch là **Serving Layer**, không phải source of truth.

Source of truth:

```text
Raw historical data = HDFS
Reference canonical = HDFS /movies/ref
```

Elasticsearch giữ các view đã tính.

## 12.1. Index names

### Batch

```text
movie_batch_stats
movie_daily_stats
genre_trends
```

### Realtime

```text
movie_realtime_stats
genre_realtime
tmdb_realtime
```

### Query outputs

```text
analytics_top_movies
analytics_genre_trends
analytics_growth
analytics_timeseries
```

Có thể dùng pattern:

```text
analytics_*
```

nhưng từng index cụ thể phải được ghi trong Query Contract.

---

## 12.2. Write ownership

| Index | Writer duy nhất |
|---|---|
| `movie_batch_stats` | Batch Loader |
| `movie_daily_stats` | Batch Loader |
| `genre_trends` | Batch Loader |
| `movie_realtime_stats` | Spark Streaming |
| `genre_realtime` | Spark Streaming |
| `tmdb_realtime` | Spark Streaming |
| `analytics_*` | Spark SQL Query Job |

Dashboard **read-only**.

Spark SQL không được sửa Batch/Realtime index.

---

## 12.3. Batch publication

Khuyến nghị publish theo version:

```text
movie_batch_stats_<batch_run_id>
```

sau khi load thành công mới switch alias:

```text
movie_batch_stats
```

Mục tiêu:

- tránh query thấy dữ liệu batch nửa cũ nửa mới;
- rollback dễ;
- không overwrite index đang phục vụ.

Nếu MVP không dùng alias, batch loader vẫn phải đảm bảo load hoàn tất trước khi Query Layer đọc.

---

# 13. Spark SQL Query Contract

Spark SQL là Query Layer bắt buộc.

Không thay bằng Hive/Impala.

## 13.1. Temp views chuẩn

```text
batch_movie
batch_movie_daily
batch_genre
rt_movie
rt_genre
tmdb_rt
```

Các tên này thuộc Query Layer, không phải Elasticsearch index name.

---

## 13.2. Merge Batch + Realtime

Semantic:

```text
Batch      : event_time < T
Realtime   : window_start >= T
T          : batch_cutoff_time
```

Pseudo logic:

```sql
WITH batch AS (...),
rt AS (
  SELECT *
  FROM rt_movie
  WHERE window_start >= :batch_cutoff_time
)
...
```

Không được:

```sql
SELECT ... FROM batch
UNION ALL
SELECT ... FROM rt
```

mà không có cutoff.

---

## 13.3. Business queries tối thiểu

Query Layer phải cung cấp kết quả cho:

1. Top phim theo interaction.
2. Rating trung bình theo phim.
3. Genre phổ biến theo ngày/tuần/tháng.
4. Xu hướng movie 7 ngày.
5. Xu hướng movie 30 ngày.
6. Week-over-week growth.
7. Top phim đang tăng.
8. Batch vs Realtime.
9. Current TMDB signal hiển thị riêng.

---

# 14. Business Metric Contract

## 14.1. `interaction_count`

Trong MovieLens branch:

```text
1 rating event = 1 interaction
```

Không được gọi là `view_count`.

---

## 14.2. `avg_rating`

```text
avg_rating = rating_sum / rating_count
```

Khi merge aggregate, không được lấy:

```text
AVG(avg_rating)
```

nếu mỗi aggregate có count khác nhau.

Phải dùng weighted average:

```text
SUM(rating_sum) / SUM(rating_count)
```

---

## 14.3. `trending_score`

Contract v1 giữ công thức:

```text
trending_score =
(current_window_count - previous_window_count)
/
(previous_window_count + 1)
```

Field input:

```text
current_window_count
previous_window_count
```

Nếu nhóm muốn thêm threshold để loại phim quá ít interaction, threshold là config riêng và phải được ghi rõ.

---

## 14.4. `growth_7d`

Định nghĩa khuyến nghị:

```text
current_7d  = tổng interaction 7 ngày hiện tại
previous_7d = tổng interaction 7 ngày liền trước

growth_7d =
(current_7d - previous_7d)
/
(previous_7d + 1)
```

Query Layer là owner của metric này.

MapReduce chỉ cung cấp daily aggregates.

Trending và growth đều do Module 7 Spark SQL tính từ aggregate; Module 5
cung cấp đủ count/sum, Module 8 chỉ hiển thị kết quả. Hai cửa sổ/khoảng so
sánh phải cùng độ dài và liền kề, dùng mốc trên timeline dữ liệu nguồn.

---

# 15. Data Quality Contract

## 15.1. Movie events

Reject/quarantine nếu:

- thiếu `event_id`;
- `movieId <= 0`;
- `userId <= 0`;
- rating ngoài `[0.5, 5.0]`;
- không parse được `event_time`;
- không parse được `ingest_time`.

## 15.2. TMDB snapshots

Reject/quarantine nếu:

- thiếu `tmdbId`;
- `popularity < 0`;
- `vote_count < 0`;
- `vote_average` ngoài `[0,10]`;
- timestamp invalid.

Áp dụng chung cho cả hai topic: đủ field required, đúng schema_version/source,
ID dương, event_id đúng SHA-256 identity, numeric hữu hạn và đúng type.
NaN/Infinity không phải dữ liệu JSON hợp lệ; không silently truncate ID/vote
count hay sửa field để qua validation.

## 15.3. Reference data

Không được drop toàn bộ movie chỉ vì IMDb/TMDB mapping null.

Bắt buộc giữ:

```text
movieId
title
genres
```

Các field enrich có thể null.

---

# 16. Deduplication & Idempotency

## 16.1. Kafka/raw

`event_id` là dedup key.

## 16.2. HDFS raw

Raw layer ưu tiên giữ lịch sử append-only.

Nếu producer retry gây duplicate, Batch job phải có chiến lược dedup theo `event_id` trước aggregate hoặc trong preprocessing.

## 16.3. Elasticsearch Realtime

Document ID phải deterministic để update cùng window thay vì tạo duplicate.

Ví dụ:

```text
movieId|window_start|window_size
```

## 16.4. Elasticsearch Batch

Một `batch_run_id` không được publish hai lần với nội dung khác nhau.

---

# 17. Error Handling Contract

## 17.1. Bad Kafka event

Không silently drop.

Đưa vào:

```text
movie-events-dlq
tmdb-snapshots-dlq
```

hoặc, nếu MVP chưa triển khai DLQ, ghi vào:

```text
/quarantine/kafka/<topic>/dt=YYYY-MM-DD/
```

Tối thiểu phải log:

```text
event_id
reason
raw_payload
detected_at
```

## 17.2. Join miss

Nếu `movieId` không tồn tại trong `movie_reference`:

- movie-level stats vẫn được tính;
- genre-level stats bỏ qua record đó hoặc gán `UNKNOWN`;
- phải count số join miss;
- Integration report phải hiển thị join-miss rate.

Với TMDB, join enrichment dùng `tmdbId` và giữ snapshot khi chưa có
MovieLens mapping; không ép tmdbId thành movieId. TMDB trending có timeline
riêng và không cần mapping để tạo `tmdb_realtime`. Reference MVP dựa trên
MovieLens nhỏ nên join miss của phim mới là trường hợp được phép. Không
thêm tùy tiện phim vào canonical reference làm phá invariant bảng gốc.

---

# 18. Module Ownership — Không chồng chéo nhiệm vụ

| Module | Được viết | Được đọc | Không được tự làm |
|---|---|---|---|
| 1. Data Source & Preparation | reference canonical, source files | source datasets | không tạo Batch/Realtime metrics |
| 2. Kafka/Ingestion | Kafka topics | prepared sources | không aggregate analytics |
| 3. HDFS | raw event storage, infrastructure cho reference | Kafka topics/reference output | không tự rebuild/publish nội dung canonical reference, không định nghĩa business metric |
| 4. MapReduce | Batch View HDFS | raw + reference | không ghi Realtime index |
| 5. Spark Streaming | Realtime View | Kafka + reference | không thay MapReduce Batch |
| 6. Elasticsearch | physical mappings/loading | Batch/RT outputs | không tính business metric mới |
| 7. Spark SQL | `analytics_*` | ES Batch + RT | không sửa upstream indices |
| 8. Dashboard | visualization | `analytics_*` | không reimplement metric |
| 9. Integration | test/report | toàn pipeline | không tự đổi contract |

---

# 19. Interface Matrix

| Producer | Output | Consumer | Contract |
|---|---|---|---|
| Data Prep | `movie_reference` | MapReduce, Spark Streaming | Section 8 |
| MovieLens Producer | `movie-events` | HDFS Sink, Spark Streaming | Section 7.1 |
| TMDB Crawler | `tmdb-snapshots` | HDFS Sink, Spark Streaming | Section 7.2 |
| HDFS Sink | `/raw/ratings` | MapReduce | Section 9 |
| HDFS Sink | `/raw/tmdb` | future Batch/TMDB jobs | Section 9 |
| MapReduce | Batch View | Batch Loader | Section 10 |
| Spark Streaming | Realtime View | Elasticsearch | Section 11 |
| Batch Loader | Batch indices | Spark SQL | Section 12 |
| Spark Streaming | RT indices | Spark SQL | Section 12 |
| Spark SQL | `analytics_*` | Dashboard | Section 13 |
| Dashboard | visualization | user | read-only |

---

# 20. Test Fixture Contract

Mỗi upstream module phải cung cấp sample nhỏ để downstream phát triển độc lập.

Thư mục đề xuất:

```text
contracts/
├── schemas/
│   ├── movie-events.schema.json
│   ├── tmdb-snapshots.schema.json
│   ├── movie-reference.schema.json
│   ├── movie-batch-stats.schema.json
│   ├── movie-daily-stats.schema.json
│   ├── genre-daily-stats.schema.json
│   ├── movie-realtime-stats.schema.json
│   └── genre-realtime.schema.json
│
├── samples/
│   ├── movie-events.sample.jsonl
│   ├── tmdb-snapshots.sample.jsonl
│   ├── movie-reference.sample.tsv
│   ├── movie-batch-stats.sample.tsv
│   └── movie-realtime-stats.sample.jsonl
│
└── README.md
```

`DATA_CONTRACT.md` chỉ có một bản chuẩn ở root. Hiện mới có ba schema và
ba sample của Module 01, kèm `fixture_provenance.json`; schema/sample Batch
và Realtime trong cây trên là output cần bổ sung bởi owner tương ứng.

### Quy tắc sample

- Ưu tiên cắt từ dữ liệu thật đã tải.
- Không random-generate dữ liệu để dùng làm bằng chứng kết quả.
- Có thể redact/giảm số lượng record.
- Sample phải validate theo schema.
- Module 01 tạo 20–100 record mỗi loại từ nguồn thật, ghi nguồn và checksum
  trong `contracts/samples/fixture_provenance.json`. Reference sample phải
  bao phủ movieId của movie event sample để consumer thử static join.
- Schema local v1.0: [movie-events](contracts/schemas/movie-events.schema.json),
  [tmdb-snapshots](contracts/schemas/tmdb-snapshots.schema.json),
  [movie-reference](contracts/schemas/movie-reference.schema.json).

---

# 21. Acceptance Test theo interface

## 21.1. Data Prep → Kafka

Pass khi:

- Producer đọc sample real data;
- event JSON đúng schema;
- topic nhận event;
- `event_time` và `ingest_time` khác nhau đúng ngữ nghĩa.

## 21.2. Kafka → HDFS

Pass khi:

- event được ghi vào đúng partition `dt=`;
- payload không mất field;
- có thể trace `event_id`.

## 21.3. HDFS → MapReduce

Pass khi:

- Job 1/2/3/4 chạy được trên sample;
- output khớp tính tay.

## 21.4. Kafka + Reference → Spark Streaming

Pass khi:

- movie aggregation cập nhật;
- static join lấy được genres;
- `genre_realtime` có dữ liệu;
- window dựa trên `event_time`.

## 21.5. Batch/Realtime → Elasticsearch

Pass khi:

- mapping đúng type;
- không duplicate document ngoài dự kiến;
- index query được.

## 21.6. Elasticsearch → Spark SQL

Pass khi:

- temp views tạo được;
- query không tham chiếu dataset chưa tồn tại;
- merge Batch + RT dùng cutoff.

## 21.7. Spark SQL → Dashboard

Pass khi:

- Dashboard chỉ đọc `analytics_*`;
- số liệu khớp query output;
- không tính lại trending_score bằng công thức khác.

---

# 22. End-to-End Trace Contract

Mỗi demo phải chọn ít nhất một event thật và trace:

```text
event_id
   │
   ▼
Kafka movie-events
   ├───────────────┐
   ▼               ▼
HDFS Raw      Spark Streaming
   │               │
   ▼               ▼
MapReduce      Realtime View
   │               │
   ▼               │
Batch View         │
   └───────┬───────┘
           ▼
    Elasticsearch
           │
           ▼
      Spark SQL
           │
           ▼
      analytics_*
           │
           ▼
       Dashboard
```

Evidence tối thiểu:

- Kafka payload;
- HDFS raw line;
- Batch/Realtime output;
- Elasticsearch document;
- Spark SQL result;
- Dashboard display.

---

# 23. Change Management

Sau khi contract được freeze:

## 23.1. Mọi thay đổi cần một Contract Change Request

Template:

```text
Change ID:
Requester:
Current contract:
Proposed change:
Reason:
Affected modules:
Backward compatible: yes/no
Migration plan:
Test plan:
Approved by:
```

## 23.2. Không được tự ý

- rename topic;
- rename field;
- đổi data type;
- đổi HDFS path;
- đổi Elasticsearch index;
- đổi metric formula;
- đổi time semantics.


---

# 24. Definition of Ready trước khi bắt đầu coding

Kickoff chỉ được coi là hoàn tất khi toàn nhóm xác nhận:

- [ ] Architecture đã thống nhất.
- [ ] Event Data vs Reference Data đã rõ.
- [ ] `movie-events` schema đã freeze.
- [ ] `tmdb-snapshots` schema đã freeze.
- [ ] `movie_reference` schema đã freeze.
- [ ] `event_time` / `ingest_time` được hiểu thống nhất.
- [ ] `batch_cutoff_time` strategy đã chốt.
- [ ] HDFS path đã chốt.
- [ ] Batch View schema đã chốt.
- [ ] Realtime View schema đã chốt.
- [ ] Elasticsearch index names đã chốt.
- [ ] Spark SQL views đã chốt.
- [ ] Metric definitions đã chốt.
- [ ] Owner của từng output đã chỉ định.
- [ ] Sample contract files đã có.
- [ ] Mỗi thành viên biết input và output của module mình.

---

# 25. Definition of Done cho toàn dự án

Pipeline chỉ được coi là hoàn thành khi:

1. MovieLens rating thật đi qua Kafka.
2. Event được lưu ở HDFS raw.
3. MapReduce tạo Batch View.
4. Cùng event được Spark Streaming xử lý.
5. Spark join được `movie_reference` để tính genre realtime.
6. Batch + Realtime được Elasticsearch phục vụ.
7. Spark SQL hợp nhất theo `batch_cutoff_time` không double count.
8. `analytics_*` trả lời được business questions.
9. Dashboard hiển thị đúng query result.
10. Một event có thể trace xuyên suốt bằng `event_id`.
11. Không module nào cần đọc source code của module khác mới hiểu output.
12. Contract tests pass.

---

# 26. Quyết định bắt buộc của nhóm trưởng trong buổi Kickoff

Nhóm trưởng phải chốt các mục sau trước khi giao task implementation:

| # | Quyết định | Giá trị Contract v1 |
|---|---|---|
| 1 | Kafka movie topic | `movie-events` |
| 2 | Kafka TMDB topic | `tmdb-snapshots` |
| 3 | Business time | `event_time` |
| 4 | Ops/arrival time | `ingest_time` |
| 5 | Raw ratings path | `/raw/ratings/dt=YYYY-MM-DD/` |
| 6 | Raw TMDB path | `/raw/tmdb/dt=YYYY-MM-DD/` |
| 7 | Reference path | `/movies/ref/current/` |
| 8 | Batch engine | Hadoop MapReduce |
| 9 | Speed engine | Spark Structured Streaming |
| 10 | Serving | Elasticsearch |
| 11 | Query engine | Spark SQL |
| 12 | Dashboard | Streamlit |
| 13 | Batch/RT boundary | `batch_cutoff_time = T` |
| 14 | Batch interval | `event_time < T` |
| 15 | Realtime interval | `event_time >= T` |
| 16 | Trend base data | `movie_daily_stats` |
| 17 | Genre enrichment | `movie_reference` static join |
| 18 | Interaction meaning | 1 rating = 1 interaction |
| 19 | Trending formula | `(current - previous)/(previous + 1)` |
| 20 | Contract change | phải qua Change Request |

---

# 27. Mental Model cuối cùng cho toàn nhóm

```text
MovieLens rating/TMDB snapshot
            │
            ▼
          Kafka
       Event Contract
            │
       ┌────┴────┐
       ▼         ▼
     HDFS      Spark
      │           │
 MapReduce     Realtime
      │           │
 Batch View       │
      └────┬──────┘
           ▼
     Elasticsearch
           │
           ▼
       Spark SQL
           │
           ▼
      analytics_*
           │
           ▼
       Dashboard
```

Reference data:

```text
MovieLens movies/links + IMDb
            │
            ▼
     movie_reference
            │
       ┌────┴────┐
       ▼         ▼
   MapReduce    Spark
```

Và nguyên tắc quan trọng nhất:

> **Mỗi module chỉ chịu trách nhiệm cho output contract của mình. Module sau không sửa dữ liệu của module trước; mọi thay đổi interface phải được thống nhất trước.**

---

# 28. Ghi chú về các điểm được làm chặt thêm so với Kickoff ban đầu

Tài liệu Contract v1 chủ động làm rõ bốn điểm để tránh lỗi khi integration:

1. **Spark Streaming dùng `event_time`, không dùng `ingest_time`, cho business window.**
2. **Bổ sung `movie_daily_stats`** để các query 7 ngày/30 ngày/WoW có dữ liệu thật để chạy.
3. **Bổ sung `batch_cutoff_time`** để Batch + Realtime không bị double count.
4. **Bổ sung dependency `movie_reference → Spark Streaming`** để tạo `genre_realtime`.

Các điểm này không thay đổi kiến trúc bắt buộc; chúng chỉ hoàn thiện interface để các module có thể ghép đúng.

---

**End of DATA_CONTRACT.md**
