# Module 01 — Data Source & Preparation

Module 01 biến MovieLens, IMDb và TMDB thành dữ liệu sạch, có provenance,
version và kiểm tra chất lượng để Kafka/Ingestion, HDFS, MapReduce và Spark
Streaming tiếp nhận. Module sở hữu canonical `movie_reference`, cung cấp
MovieLens normalizer và TMDB fetch-once; replay timing, scheduler và gửi
Kafka thuộc Module 02.

Interface nền: [DATA_CONTRACT.md](../../DATA_CONTRACT.md).
Thiết kế từng bước: [MODULE_01_DATA_SOURCE_PREPARATION.md](MODULE_01_DATA_SOURCE_PREPARATION.md).
Schema/serialization/sample: [contracts/README.md](../../contracts/README.md).

## Trạng thái bàn giao

Kết quả local ngày **08/10/2026**:

| Hạng mục | Kết quả |
| --- | --- |
| Bước 1–15 | Hoàn thành môi trường, download/provenance, profiling, ID normalization, reference, sort ratings, adapters, schemas, fixtures và manifest. |
| Reference | 9.742 phim; `ref_version=20261004-5b4e04a2`; LEFT JOIN giữ đủ phim MovieLens. |
| Replay | 100.836 ratings, timestamp tăng không giảm, nội dung nguồn giữ nguyên. |
| TMDB | Snapshot thật 20 phim từ `/trending/movie/day`; một snapshot time chung. |
| Fixtures | Ba loại, mỗi loại 20 record thật, schema-valid; sample movie-event join được với sample reference. |
| Kiểm thử | 133 tests PASS; profiling, quality gate và audit local PASS. |
| Contract | `1.0-draft` đã đối chiếu; chưa ghi nhận nhóm freeze. Event vẫn dùng `schema_version="1.0"`. |
| Bước 16 | Chờ HDFS endpoint/path để publish reference. |
| Bước 17 | Chờ Module 2/4/5 xác nhận consume; chưa có Kafka/MapReduce/Spark smoke test thực tế. |

8 phim thiếu tmdbId, 23 phim không match IMDb và 34 phim có
`(no genres listed)` được giữ trong reference theo thiết kế. Snapshot TMDB
hiện có **0/20 phim** match reference MovieLens nhỏ: downstream phải xử lý
unmatched tmdbId và giữ timeline TMDB riêng.

Báo cáo: [REVIEW_MODULE_01.md](REVIEW_MODULE_01.md), cùng
`data/reports/module_01_audit.json`. Số liệu trên là bản ghi của lần kiểm
tra hiện tại; khi rebuild, đọc manifest/report mới để biết version thực.

## Chọn cách chạy

Tất cả lệnh dưới đây chạy từ thư mục gốc `movie-trend`. Xem danh sách lệnh
ngay trong terminal:

```bash
make -C modules/data_source help
```

| Tình huống | Lệnh | Điều kiện và kết quả |
| --- | --- | --- |
| Chuẩn bị môi trường lần đầu | `make -C modules/data_source setup env` | Tạo `.venv` nếu thiếu, cài requirements và tạo `.env` nếu thiếu; sau đó điền API key. |
| Chạy từ nguồn ban đầu | `make -C modules/data_source pipeline` | Download hoặc reuse raw → prepare → crawl TMDB → fixtures/manifest/tests/audit. Cần mạng và API key. |
| Dựng lại từ dữ liệu local | `make -C modules/data_source run` | Cần raw/provenance và snapshot có sẵn; dựng lại reference, sort ratings rồi hoàn thiện artifacts/tests/audit. |
| Kiểm tra artifacts hiện tại | `make -C modules/data_source verify` | Cần raw/prepared/snapshot; không rebuild reference hoặc sort lại, tạo lại fixtures/manifest/reports rồi test/audit. |
| Chỉ kiểm thử code | `make -C modules/data_source test` | Chạy toàn bộ tests, không download hoặc gọi API thật. |

`pipeline` và `run` ghi lại canonical reference với build metadata mới.
`verify` đọc/đối chiếu reference hiện tại. Các workflow chạy tuần tự và
dừng khi một bước lỗi, kể cả khi gọi với `make -j`.

**Chỉ phát triển consumer bằng sample:** làm setup Python dưới đây, rồi
chạy snippet validate/join trong [contracts/README.md](../../contracts/README.md).
Không cần download dataset lớn hoặc API key để đọc sample.

**Máy mới chưa có dữ liệu:** làm setup và chạy pipeline lần đầu ở mục tiếp
theo. `data/raw/`, `data/prepared/` và `.env` bị Git ignore, nên không
có sẵn chỉ từ việc clone code.

## Setup và cấu hình

Môi trường đã kiểm tra: Python **3.12**, GNU make và thư viện trong
[requirements.txt](requirements.txt). Tạo môi trường trên máy mới:

```bash
make -C modules/data_source setup env
```

Target `env` lấy nội dung từ [.env.example](.env.example), giữ nguyên `.env`
đang có và tạo file mới với quyền riêng cho user. Chỉnh `.env` ở root, điền API
Key v3 của bạn; không dùng API Read Access Token cho biến TMDB_API_KEY.

| Biến | Mặc định | Tác dụng |
| --- | --- | --- |
| `MOVIELENS_VARIANT` | `ml-latest-small` | Chọn nguồn MovieLens; dùng variant này cho MVP. |
| `TMDB_API_KEY` | Không có | Bắt buộc khi gọi API thật; không cần cho tests/sample reader. |
| `TMDB_BASE_URL` | Tự thử hai host | Optional: `https://api.themoviedb.org/3` hoặc `https://api.tmdb.org/3`. |
| `HTTPS_PROXY` | Môi trường requests | Dùng nếu mạng cần proxy HTTPS. |

Environment có ưu tiên cao hơn `.env`. Các script tìm `.env` theo vị trí
dự án; [config.yaml](config.yaml) hiện mô tả required/optional files, chưa
được đọc như cấu hình runtime. Không đổi YAML đơn lẻ để chọn variant.

Dữ liệu IMDb được giữ nén và đọc theo chunk. Cần có dung lượng cho dataset
nén, staging download, `.venv` và file prepared. Downloader từ chối ghi đè
raw đã có. Target `download` reuse nguồn đã đủ required files; nguồn bị
thiếu một phần sẽ báo lỗi để điều tra. Nguồn reuse phải đi kèm provenance
download gốc trong manifest. Quy trình chuyển dataset/version mới cần
được thống nhất trước khi thay nguồn hiện tại.

## Pipeline lần đầu

Sau khi setup và điền API key, chạy một lệnh:

```bash
make -C modules/data_source pipeline
```

| Phase | Các bước chạy theo thứ tự |
| --- | --- |
| `download` | `download-movielens` → `download-imdb` |
| `prepare` | `profile` → `build-reference` → `validate-reference` → `sort-ratings` |
| `tmdb` | Crawl một snapshot mới |
| `finish` | `fixtures` → `manifest` → `test` → `audit` |

`run` chạy `prepare` → `finish`, dùng snapshot hiện có. Nếu chỉ vừa crawl
hoặc sửa artifact, dùng `finish` để cập nhật sample/manifest và kiểm tra.
Makefile dùng `python -m` từ root nên có thể gọi cả bằng
`make -C modules/data_source <target>` hoặc `cd modules/data_source` rồi
`make <target>`, không cần activate `.venv`.

Checksum mismatch phải được điều tra; không sửa provenance để che việc raw
thay đổi. Sau lỗi, sửa nguyên nhân rồi chạy lại workflow hoặc phase cần thiết.

## Vai trò và cách chạy toàn bộ file trong src

Không phải mọi file Python đều là chương trình độc lập. Các thư viện dùng
chung được import trong pipeline và kiểm tra qua tests; chạy chúng bằng
`python file.py` sẽ không tạo artifact. Makefile bao phủ cả 10 script CLI:

| File | Vai trò | Target Makefile |
| --- | --- | --- |
| [download_movielens.py](src/download_movielens.py) | Download MovieLens, checksum và provenance | `download-movielens` |
| [download_imdb.py](src/download_imdb.py) | Download/verify hai file IMDb nén | `download-imdb` |
| [profile_sources.py](src/profile_sources.py) | Profile raw và hard checks | `profile` |
| [build_movie_reference.py](src/build_movie_reference.py) | LEFT JOIN, tạo canonical fields/version và ghi TSV | `build-reference` |
| [validate_reference.py](src/validate_reference.py) | Quality gate và coverage reference | `validate-reference` |
| [sort_ratings.py](src/sort_ratings.py) | Sort replay input và kiểm tra dữ liệu không đổi | `sort-ratings` |
| [tmdb_source_adapter.py](src/tmdb_source_adapter.py) | Fetch-once CLI và normalizer dùng chung cho producer | `tmdb` |
| [build_fixtures.py](src/build_fixtures.py) | Cắt/validate ba sample thật và ghi provenance | `fixtures` |
| [build_manifest.py](src/build_manifest.py) | Xác minh nguồn và hoàn thiện manifest | `manifest` |
| [audit_module.py](src/audit_module.py) | Kiểm tra toàn bộ artifact và local handoff | `audit` |
| [normalize_ids.py](src/normalize_ids.py) | Thư viện normalize movieId, imdbId, tmdbId | Được `build-reference` dùng; kiểm tra qua `test-ids`/`test-reference` |
| [movie_event_adapter.py](src/movie_event_adapter.py) | Thư viện `to_movie_event(row, ingest_time)` | Được `fixtures`/`audit` và Module 2 dùng; kiểm tra qua `test-movie-event` |
| [contract_validation.py](src/contract_validation.py) | Thư viện đọc JSONL/TSV, validate schema và atomic write | Được `fixtures`/`manifest`/`audit` dùng; kiểm tra qua `test-contracts` |
| [src/__init__.py](src/__init__.py) | Khai báo Python package | Không có CLI; hỗ trợ imports |

Có thể chạy từng target từ root, ví dụ:

```bash
make -C modules/data_source build-reference
make -C modules/data_source sort-ratings
```

Target riêng chỉ thực hiện bước đó; dùng workflow tổng hợp để chạy đủ các
bước phụ thuộc và cập nhật metadata sau khi artifact thay đổi.

## Output và ý nghĩa

Mọi path dưới đây tính từ root repository:

| Output | Dùng để làm gì? |
| --- | --- |
| `data/raw/movielens/<variant>/` | Movies, links, ratings và các file download gốc. |
| `data/raw/imdb/*.tsv.gz` | Hai nguồn IMDb nén, được đọc trực tiếp theo chunk. |
| `data/prepared/reference/movie_reference.tsv` | Reference đầy đủ, UTF-8 TSV có header và 10 canonical fields. |
| `data/prepared/movielens/ratings_sorted.csv` | CSV có header userId/movieId/rating/timestamp; sắp theo timestamp ASC. |
| `data/prepared/tmdb/snapshot_<UTC timestamp>.jsonl` | Snapshot đã normalize, mỗi dòng một phim. |
| `contracts/schemas/*.schema.json` | Ba schema v1.0 kiểm tra event/reference. |
| `contracts/samples/*` | Ba fixture thật và `fixture_provenance.json`. |
| `data/reports/source_manifest.json` | Provenance, SHA-256/size, reference version/row count, prepared files và fixture metadata. |
| `data/reports/data_profile.json` | Profile nguồn và hard failures. |
| `data/reports/data_quality_report.json` | Reference hard checks và coverage. |
| `data/reports/module_01_audit.json` | Kết quả audit, warnings và pending handoff. |

Reference giữ đủ MovieLens movies, dùng LEFT JOIN links/IMDb; năm ưu tiên
IMDb rồi fallback title MovieLens. Title/genres nguồn được giữ nguyên.
Null được serialize thành ô trống đúng cột nullable. Ref version là
`YYYYMMDD-<8 ký tự source hash>`; manifest ghi full source hash/checksum.

Manifest kiểm tra checksum và size nguồn so với provenance download, giữ
retrieval time/archive checksum gốc. Các source được nhóm trong
`sources[].files[]`; `name` được giữ để tương thích, `filename` và
metadata từng file được bổ sung. TMDB source_url là canonical endpoint
không có API key; artifact là normalized snapshot.

## Interface cho Module 02

Import từ package dùng chung; không tự viết lại field mapping hoặc hash:

```python
from modules.data_source.src.movie_event_adapter import to_movie_event
from modules.data_source.src.tmdb_source_adapter import (
    fetch_trending_movies, normalize_tmdb_item, utc_now,
)

# row lấy từ csv.DictReader(ratings_sorted.csv).
# Gọi trong producer ngay trước lúc gửi Kafka.
movie_event = to_movie_event(row, ingest_time=utc_now())
# Kafka key: str(movie_event["movieId"]), encode UTF-8.

raw_items, snapshot_time = fetch_trending_movies(api_key)
for item in raw_items:
    tmdb_event = normalize_tmdb_item(
        item, event_time=snapshot_time, ingest_time=utc_now()
    )
    # Kafka key: str(tmdb_event["tmdbId"]), encode UTF-8.
    # Producer của Module 02 gửi tmdb_event tại đây.
```

Snippet minh họa lời gọi API, giả định row/api_key do producer cung cấp;
không phải một Kafka producer chạy sẵn. `fetch_trending_movies` trả
`(raw_items, snapshot_event_time)`; `normalize_tmdb_item`/to_movie_event
trả dictionary v1.0. Producer serialize JSON bằng UTF-8, cập nhật ingest
time thực và giữ event_time/event_id khi retry. Module 02 quyết định tốc
độ replay và scheduler TMDB; Module 01 chạy một snapshot mỗi lần gọi CLI.

## Bàn giao cho HDFS, MapReduce và Spark

| Consumer | Nhận gì? | Acceptance tiếp theo |
| --- | --- | --- |
| Module 2 Kafka | Sorted ratings, hai adapters, hai event schemas và JSONL mẫu | Payload Kafka schema-valid, key đúng, source time/ingest time đúng nghĩa. |
| Module 3 HDFS | Canonical reference, ref_version và manifest | Cung cấp endpoint; phối hợp Module 1 upload/verify reference version trước khi cập nhật current. |
| Module 4 MapReduce | Reference TSV/sample, schema và genres rule | Đọc TSV đúng type/null; join qua movieId, tính count/sum từ raw Kafka events trên HDFS. |
| Module 5 Spark | Canonical reference, hai event schemas/samples | Đặt UTC, parse event_time, chạy static join movieId -> genres; giữ record TMDB chưa có mapping. |

HDFS target dự kiến:
`/movies/ref/version=<ref_version>/movie_reference.tsv` và
`/movies/ref/current/movie_reference.tsv`. Module 01 sở hữu nội dung và
publish reference; Module 03 quản lý infrastructure. Hiện chưa publish và
chưa chạy acceptance test của các consumer thật.

## Chạy toàn bộ tests hoặc từng file test

| File test | Kiểm tra chính | Target Makefile |
| --- | --- | --- |
| [test_ids.py](tests/test_ids.py) | ID normalization, missing mapping và IMDb ID dài | `test-ids` |
| [test_reference.py](tests/test_reference.py) | LEFT JOIN, release year, canonical fields và ref version | `test-reference` |
| [test_movie_event.py](tests/test_movie_event.py) | MovieLens event mapping, time semantics và deterministic ID | `test-movie-event` |
| [test_tmdb_snapshot.py](tests/test_tmdb_snapshot.py) | TMDB normalization/fetch, fallback HTTP và writer | `test-tmdb` |
| [test_contract_artifacts.py](tests/test_contract_artifacts.py) | Schema, fixture extraction, provenance và manifest failure paths | `test-contracts` |
| [tests/__init__.py](tests/__init__.py) | Khai báo package tests, không chứa bài kiểm thử | Không có target riêng |

Lệnh thường dùng chạy toàn bộ 5 file:

```bash
make -C modules/data_source test
```

Khi cần kiểm tra riêng một nhóm hoặc lọc test:

```bash
make -C modules/data_source test-ids
make -C modules/data_source test-reference
make -C modules/data_source test-movie-event
make -C modules/data_source test-tmdb
make -C modules/data_source test-contracts
make -C modules/data_source test-tmdb PYTEST_ARGS='-q -k popularity'
```

`test` bao phủ cả helpers/adapters không có CLI. Tests dùng sample thật
trong `contracts/samples` và dữ liệu nhỏ trong thư mục tạm; không cần API
key, Kafka/HDFS/Spark hoặc dataset lớn. Audit/verify cần dữ liệu local đầy đủ.

## Tùy chọn Makefile

| Biến | Mặc định | Cách dùng |
| --- | --- | --- |
| `VENV` | `<root>/.venv` | Chọn thư mục virtualenv; `setup` tạo ở đây nếu thiếu. |
| `BOOTSTRAP_PYTHON` | `python3` | Python dùng để tạo virtualenv. |
| `PYTHON` | `<VENV>/bin/python` | Chọn interpreter đã có cho tất cả script/test. |
| `PYTEST_ARGS` | `-q` | Truyền tùy chọn pytest, ví dụ `-v` hoặc `-q -k popularity`. |
| `FIXTURE_COUNT` | `20` | Số sample mỗi loại; cho phép 20–100, không vượt số record thật. |
| `SNAPSHOT` | Để trống | Chọn snapshot cụ thể, path tính từ root; mặc định chọn file snapshot mới nhất. |
| `INGEST_TIME` | Để trống | Fixture ingest time cố định; mặc định thời gian tạo mẫu. |

Ví dụ hoàn thiện sample/manifest/tests/audit với nguồn và thời gian cố định:

```bash
make -C modules/data_source finish \
  FIXTURE_COUNT=20 \
  SNAPSHOT=data/prepared/tmdb/snapshot_20261008T104404Z.jsonl \
  INGEST_TIME=2026-10-08T11:00:00Z
```

Các biến được truyền xuống các phase của `pipeline`, `run`, `finish` và
`verify`. Không cần gõ lại tham số cho từng script.

Dùng `make -C modules/data_source <target>` từ root. Có thể override
`PYTHON=/path/to/python`. `verify` không download/crawl lại nhưng ghi lại
sample/manifest/report; `audit` ghi audit report. Khi audit PASS, warnings
coverage và các mục pending integration vẫn cần được đọc.

## Xử lý lỗi

| Lỗi | Cách xử lý |
| --- | --- |
| `ModuleNotFoundError` | Dùng Python trong .venv và `python -m` từ root. |
| Thiếu source/prepared artifact | Chạy bước tạo input tương ứng; clone không bao gồm dataset lớn. |
| Raw đã tồn tại hoặc thiếu một phần | Make reuse nguồn đầy đủ; nếu bị thiếu một phần, downloader dừng để bảo vệ raw và cần điều tra nguồn. |
| Checksum/size mismatch | Đối chiếu provenance và phiên bản nguồn; điều tra trước khi rebuild. |
| Fixture thiếu record | Count phải 20–100 và không vượt số dòng thật trong snapshot/nguồn. |
| TMDB `401/403` | Kiểm tra API Key v3 và quyền truy cập. |
| TMDB `429` | Đợi trước khi chạy lại. |
| TMDB reset/DNS/timeout | Mặc định tự thử api.tmdb.org; có thể chọn host bằng TMDB_BASE_URL hoặc kiểm tra mạng/proxy. |
| TLS certificate error | Kiểm tra đồng hồ, CA và proxy HTTPS. |
| Hết dung lượng | Kiểm tra dung lượng disk/staging trước khi download, build hoặc chạy tests. |

Module 01 không thực hiện aggregation Batch/Realtime hay quản lý raw
Kafka storage. Cơ chế DLQ/quarantine downstream và tiêu chí integration
được mô tả trong [DATA_CONTRACT.md](../../DATA_CONTRACT.md).
