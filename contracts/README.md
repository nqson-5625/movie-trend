# Contracts — schema và dữ liệu mẫu để tích hợp

Thư mục này cung cấp interface kiểm tra được bằng máy và mẫu từ dữ liệu
thật để Module 2, 4 và 5 phát triển consumer độc lập. Contract nền duy nhất
là [DATA_CONTRACT.md](../DATA_CONTRACT.md), hiện ở trạng thái **1.0-draft**.
Ba schema local triển khai **event schema_version `1.0`**; trạng thái draft
của tài liệu không phải giá trị được gửi trong event.

## Có gì và dùng cho ai?

| Artifact | Format | Consumer | Trạng thái |
| --- | --- | --- | --- |
| [movie-events.schema.json](schemas/movie-events.schema.json) | JSON Schema Draft 2020-12 | MovieLens producer, HDFS sink, Spark Streaming | Đã có |
| [tmdb-snapshots.schema.json](schemas/tmdb-snapshots.schema.json) | JSON Schema Draft 2020-12 | TMDB producer, HDFS sink, Spark Streaming | Đã có |
| [movie-reference.schema.json](schemas/movie-reference.schema.json) | Schema cho logical reference row | MapReduce, Spark static join | Đã có |
| [movie-events.sample.jsonl](samples/movie-events.sample.jsonl) | UTF-8 JSONL | Consumer event và sample join | 20 record thật |
| [tmdb-snapshots.sample.jsonl](samples/tmdb-snapshots.sample.jsonl) | UTF-8 JSONL | Consumer TMDB | 20 record thật |
| [movie-reference.sample.tsv](samples/movie-reference.sample.tsv) | UTF-8 TSV có header | Reference reader và sample join | 20 record thật, thêm một header |
| [fixture_provenance.json](samples/fixture_provenance.json) | JSON | Integration/audit | Nguồn, checksum và thời gian tạo mẫu |

Mẫu hiện có đã validate và đối chiếu nguồn. Schemas/sample của Batch,
Realtime và Analytics chưa được triển khai; owner các module đó bổ sung
theo contract nền. Các sample hiện tại là dữ liệu kiểm tra giao diện,
không chứng minh Kafka/HDFS/Spark đã chạy end-to-end.

## Quy tắc chung khi đọc record

- Event JSONL: mỗi dòng là một JSON object; không có header hoặc mảng JSON
  bao quanh. Numeric field là JSON number, không phải string.
- Event có đủ các field của v1.0; field thừa bị reject
  (`additionalProperties=false`). Validate schema theo `schema_version`
  trước khi parse/aggregate; payload v1.1 cần validator riêng.
- Timestamp là string UTC dạng `YYYY-MM-DDTHH:MM:SSZ`; ngày/giờ phải hợp
  lệ. Consumer parse thành timestamp và dùng UTC.
- ID integer phải dương; không đổi null mapping thành `0`. NaN/Infinity
  và việc truncate số thập phân để thành ID/vote count bị từ chối.
- `event_time` là thời gian nguồn; `ingest_time` là thời gian Kafka
  producer gửi. Ingest time trong fixture là thời gian xử lý local và phải
  được producer thay bằng giá trị thực khi gửi event.

## MovieLens: movie-events

```text
schema_version, event_id, source, userId, movieId, rating, event_time, ingest_time
```

`schema_version="1.0"`, `source="movielens"`; userId/movieId là integer
`> 0`; rating trong `[0.5,5.0]`. Event time được chuyển từ Unix epoch
seconds gốc của rating sang UTC.

Identity:

```text
event_id = sha256("movielens|userId|movieId|original_timestamp")
```

Hash UTF-8 trả về lowercase hex 64 ký tự. Ingest time và rating không tham
gia identity. Retry cùng source row giữ event_id; replay giữ event_time.
Kafka key do Module 2 serialize từ movieId.

## TMDB: tmdb-snapshots

```text
schema_version, event_id, source, tmdbId, title, popularity,
vote_count, vote_average, event_time, ingest_time
```

`schema_version="1.0"`, `source="tmdb"`; tmdbId là integer `> 0`,
title non-empty, popularity hữu hạn `>= 0`, vote_count là integer `>= 0`,
vote_average trong `[0,10]`.

```text
event_id = sha256("tmdb|tmdbId|event_time")
```

Event time là thời điểm lấy snapshot và dùng chung cả snapshot. Kafka key
là tmdbId. Module 2 dùng fetch/normalizer của Module 1 rồi truyền ingest
time thực tế trước khi gửi.

**Mapping/timeline:** snapshot hiện tại có 0/20 tmdbId trong canonical
MovieLens reference. Nhánh TMDB xử lý riêng; giữ snapshot khi enrichment
không có mapping. Không ép tmdbId thành movieId, không INNER JOIN để loại
record, không cộng popularity vào historical interaction count.

## Reference: movie-reference

Header TSV theo đúng thứ tự:

```text
movieId	title	release_year	genres	imdbId	tmdbId	imdb_rating	imdb_votes	ref_version	generated_at
```

JSON Schema mô tả **logical row sau khi decode TSV**, không validate trực
tiếp một dòng text.

| Field | Logical type | Quy tắc |
| --- | --- | --- |
| movieId | integer | Required, unique, > 0 |
| title | string | Non-empty; giữ nguyên title/năm MovieLens |
| release_year | integer hoặc null | IMDb startYear, fallback năm cuối title |
| genres | string | Non-empty, phân tách bằng pipe; giữ `(no genres listed)` |
| imdbId | string hoặc null | `^tt[0-9]{7,}$`, không truncate ID dài |
| tmdbId | integer hoặc null | > 0 khi có giá trị |
| imdb_rating | number hoặc null | Hữu hạn, `[0,10]` |
| imdb_votes | integer hoặc null | >= 0 |
| ref_version | string | `YYYYMMDD-<8 ký tự source hash>`, chung cả build |
| generated_at | UTC timestamp string | Chung cả build |

Các cột nullable vẫn có trong header; ô trống được decode thành null.
Dùng `csv.DictReader(..., delimiter="\t")` hoặc TSV reader hỗ trợ quoting;
không split dòng thủ công vì title có thể được quote.

Reference mẫu bao phủ mọi movieId của movie event mẫu. Với dữ liệu đầy đủ,
consumer vẫn phải theo dõi join miss và giữ movie-level record theo
contract. Trước khi explode genre, xử lý `(no genres listed)` theo policy
consumer; không sửa canonical value.

## Validate và thử join mà không cần Kafka/HDFS/Spark

Cần Python 3.12 và môi trường của dự án. Nếu máy mới chưa có `.venv`,
tạo từ thư mục gốc repository:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r modules/data_source/requirements.txt
```

Nếu đã có môi trường, dùng trực tiếp. Chạy từ thư mục gốc; lệnh chỉ đọc schema
và sample, không cần dataset lớn hoặc API key:

```bash
.venv/bin/python - <<'PY'
from pathlib import Path
from modules.data_source.src.contract_validation import (
    load_validator, read_jsonl, read_reference, reference_record,
    validate_record, validate_reference_rows,
)

samples = Path("contracts/samples")
events = read_jsonl(samples / "movie-events.sample.jsonl")
tmdb = read_jsonl(samples / "tmdb-snapshots.sample.jsonl")
for name, records in (("movie-events", events), ("tmdb-snapshots", tmdb)):
    validator = load_validator(name)
    for record in records:
        validate_record(record, validator)
    print(name, len(records), "valid records")

rows = read_reference(samples / "movie-reference.sample.tsv")
validate_reference_rows(rows)
by_id = {record["movieId"]: record for record in map(reference_record, rows)}
assert all(event["movieId"] in by_id for event in events)
print("movie-reference", len(rows), "valid rows; all sample movieIds join")
PY
```

Helper bật kiểm tra ngày UTC thực và parse null/numeric đúng type. Khi dùng
validator riêng, cần bật format validation; chỉ kiểm tra regex timestamp
không phát hiện ngày không tồn tại. Với Spark, đặt session timezone UTC,
đọc TSV theo schema tường minh và dùng `event_time` làm event-time column.
Snippet trên là smoke test local; Module 5 vẫn cần chạy static join thực
bằng Spark.

## Tạo lại sample và metadata

Owner tạo sample là Module 01. Cần có sorted ratings, canonical reference
và một snapshot TMDB thật trong `data/prepared/`:

```bash
.venv/bin/python -m modules.data_source.src.build_fixtures
.venv/bin/python -m modules.data_source.src.build_manifest
```

`build_fixtures` mặc định lấy 20 dòng, validate trước khi ghi, ưu tiên
reference chứa movieId của event mẫu và bổ sung missing mapping/genre
sentinel nếu còn chỗ. Provenance ghi nguồn và SHA-256; manifest bổ sung
checksum/số dòng của output sample.

Có thể chọn nguồn và thời gian cố định để tái tạo cùng mẫu MovieLens:

```bash
.venv/bin/python -m modules.data_source.src.build_fixtures \
  --count 20 \
  --snapshot data/prepared/tmdb/snapshot_20261008T104404Z.jsonl \
  --ingest-time 2026-10-08T11:00:00Z
.venv/bin/python -m modules.data_source.src.build_manifest
```

Tên snapshot trên là artifact đang có ở máy đã chuẩn bị dữ liệu; máy mới
clone phải tạo snapshot trước. Count cho phép 20–100 và không được vượt
số record thật của nguồn. TMDB sample giữ nguyên snapshot time/event_id.

Để xem cách chuẩn bị dataset, reuse adapters và kiểm tra toàn Module 01,
đọc [modules/data_source/README.md](../modules/data_source/README.md).
