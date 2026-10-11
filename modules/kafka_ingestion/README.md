# Module 02 — Kafka ingestion (M02-01 → M02-14)

Module này đưa **rating interactions MovieLens** và **snapshot trending TMDB**
thật từ Module 01 vào Kafka; consumer kiểm tra contract và lưu audit local.
Không coi rating là lượt xem. Schema dùng nguyên bản tại
[`contracts/schemas`](../../contracts/schemas), theo
[`DATA_CONTRACT.md`](../../DATA_CONTRACT.md) (vẫn là draft của nhóm).

## Cấu trúc thư mục

```text
modules/kafka_ingestion/
├── src/              # Python: producers, consumer, replay, lifecycle và benchmark
├── tests/            # Unit tests và integration tests
├── config/           # Cấu hình topics
├── reports/          # Báo cáo kiểm thử và hiệu năng
├── Makefile
├── requirements.txt
├── .env.example
├── setup_kafka_wsl.sh
├── README.md
└── README_SETUP_KAFKA_WSL.md
```

Chạy Python module từ repository root, ví dụ:
`python -m modules.kafka_ingestion.src.movie_producer --help`.
Các lệnh Makefile bên dưới dùng package `src` này.

## Trạng thái và bằng chứng

Kiểm tra ngày 11/10/2026 trên WSL2 Ubuntu, Python 3.12, Kafka local KRaft.

| Hạng mục | Kết quả |
| --- | --- |
| M02-01–06 | Script setup WSL đã có; binary Kafka và broker local chạy được. Không cài/format lại storage cũ. |
| M02-07 | Topic provisioning/lifecycle đã có; kiểm tra hai topic, 1 partition, RF=1 thành công. |
| M02-08 | Producer gửi 100 ratings từ `ratings_sorted.csv`; callback ACK đủ 100. |
| M02-09 | Gọi API TMDB thật qua adapter Module 01, gửi 20 phim, callback ACK đủ 20. |
| M02-10 | Consumer đọc 120/120 hợp lệ, không trùng/lỗi, committed lag = 0 cho cả hai topic. |
| M02-11 | Batch, rate limit, checkpoint bền vững, retry có giới hạn, SIGINT/SIGTERM và phát hiện trùng/conflict. |
| M02-12 | Tests dùng Kafka thật riêng biệt: E2E, restart/offset, duplicates, JSON/schema lỗi, mất kết nối, retry khi broker restart, SIGTERM/resume. |
| M02-13 | Đã đo 10/100/500 events/s trên WSL2 được cấp khoảng 4 GB RAM. Xem [báo cáo](reports/PERFORMANCE.md). |
| M02-14 | Makefile, dependencies pin, `.env.example`, hướng dẫn clone/setup/replay/tests đầy đủ. |

Lượt kiểm thử cuối: **169 passed, 0 failed, 0 skipped** (gồm 5 integration
tests Kafka thật), 42,29 giây. [JUnit XML](reports/tests.xml).

Bằng chứng: [acceptance.json](reports/acceptance.json),
[performance.json](reports/performance.json). Đây là kiểm tra Module 01 → 02,
chưa phải kiểm chứng HDFS/Spark/Elasticsearch hoặc toàn bộ Lambda pipeline.

## Cài đặt từ máy mới clone

Chạy trong filesystem Linux của WSL2 (ví dụ `~/movie-trend`), cần Python 3.12,
`python3-venv`, Java 17 và dung lượng đủ cho dữ liệu Module 01.

```bash
# Tại repository root:
make -C modules/kafka_ingestion setup
make -C modules/data_source env
```

`setup` tạo `.venv` nếu thiếu và cài lock dependencies dùng chung Module 01
cộng `confluent-kafka==2.13.2`. Điền `TMDB_API_KEY` vào `.env` ở root; hợp nhất
các giá trị cần thiết từ [`.env.example`](.env.example), không ghi đè secret
đã có. Biến môi trường shell được ưu tiên hơn `.env`. Không in/commit key.

Chuẩn bị nguồn theo [Module 01](../data_source/README.md). Đường chạy đầy đủ:

```bash
make -C modules/data_source pipeline
```

Pipeline này tải cả MovieLens/IMDb và gọi TMDB, có thể lâu. Nếu chỉ cần input
cho ingestion trên máy mới, có thể chạy các target riêng sau (không tạo
canonical movie reference đầy đủ):

```bash
make -C modules/data_source download-movielens
make -C modules/data_source sort-ratings
make -C modules/data_source tmdb
```

Input producer là `data/prepared/movielens/ratings_sorted.csv` với header
`userId,movieId,rating,timestamp`, đọc tuần tự bằng CSV iterator; không nạp toàn
bộ dataset vào RAM. Test TMDB offline dùng snapshot thật đã lưu bởi Module 01.

Cài broker **trên từng máy**, theo [hướng dẫn WSL](README_SETUP_KAFKA_WSL.md):

```bash
bash modules/kafka_ingestion/setup_kafka_wsl.sh
make -C modules/kafka_ingestion start
make -C modules/kafka_ingestion topics
make -C modules/kafka_ingestion topics-check
```

Không sao chép `~/kafka-local/data`, `cluster.id`, checkpoints hoặc consumer
audit từ máy khác. Mỗi máy tự tạo cluster và replay từ nguồn Module 01.
Script setup chỉ format thư mục dữ liệu mới/rỗng; `start/stop` không format.
`topics` tạo topic thiếu và từ chối partition/RF sai, không xóa/chỉnh topic cũ.

## Chạy producer và consumer

```bash
# Terminal 1: dừng sau 60 giây không nhận message (có thể tăng nếu cần).
make -C modules/kafka_ingestion consumer ARGS='--idle-timeout 60'

# Terminal 2: 1000 ratings mới tính từ checkpoint hiện tại.
make -C modules/kafka_ingestion producer RATE=100 BATCH_SIZE=100 LIMIT=1000
make -C modules/kafka_ingestion tmdb

# Scheduler TMDB mỗi 600 giây, Ctrl+C để dừng:
make -C modules/kafka_ingestion tmdb ARGS='--loop --interval 600'

# Replay đến EOF, có giới hạn tốc độ:
make -C modules/kafka_ingestion replay RATE=100 LIMIT=0
make -C modules/kafka_ingestion status
make -C modules/kafka_ingestion stop
```

`producer` và `replay` là cùng một chương trình. `LIMIT` tính số record trong
lần chạy, không phải số dòng tuyệt đối của file. `ARGS='--help'` xem đầy đủ CLI.
Mọi CLI có `--bootstrap`; mặc định lấy `KAFKA_BOOTSTRAP_SERVERS`, rồi
`127.0.0.1:9092`. Implementation hiện dành cho broker PLAINTEXT local; khi triển
khai remote cần bổ sung TLS/SASL, ACL và replication phù hợp.

| Chương trình | Tùy chọn chính |
| --- | --- |
| `movie_producer` | `--ratings`, `--checkpoint`, `--rate`, `--batch-size`, `--limit`, `--timeout`, `--report` |
| `tmdb_producer` | `--state`, `--interval`, `--retries`, `--timeout`, `--loop` |
| `consumer` | `--group`, `--audit`, `--limit`, `--idle-timeout`, `--report` |
| `benchmark` | `--report` |

## Contract và delivery policy

- Kafka key: UTF-8 decimal `movieId` cho `movie-events`, `tmdbId` cho
  `tmdb-snapshots`. Không dùng event_id làm key và không bật log compaction.
- Module 01 tạo deterministic `event_id`; producer/consumer xác minh lại ID,
  schema Draft 2020-12, format ngày giờ thực và key. Consumer reject JSON lỗi,
  field trùng, NaN/Infinity, schema lạ hoặc payload sai identity.
- MovieLens `event_time` giữ timestamp nguồn; TMDB giữ thời điểm fetch snapshot.
  `ingest_time` UTC được tạo ngay khi đưa từng record vào producer.
- `enable.idempotence=true`, `acks=all`, lz4, queue tối đa 8 MiB, linger 5 ms.
  Callback được xử lý qua poll/flush; enqueue thành công chưa được tính là ACK.
- Retry kết nối do librdkafka thực hiện trong `--timeout` (mặc định 30 giây),
  backoff 200 ms. Hết thời gian hoặc lỗi terminal: exit 1, in số ACK/fail/pending,
  checkpoint của batch lỗi không tiến lên. Chạy lại lệnh để retry nguồn đó.
- Một broker RF=1 chịu được graceful restart với storage còn nguyên; không
  bảo đảm chịu mất disk/máy. `acks=all` ở đây chỉ có một replica.

API reference cho callback, flush và commit:
[Confluent Python client](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html).

## Resume, duplicates và shutdown

State mặc định dưới `data/local/kafka_ingestion/` (Git ignore):

| File | Ý nghĩa |
| --- | --- |
| `movie-checkpoint.json` | SHA-256 input, bootstrap/topic, next_row sau batch đã ACK toàn bộ. |
| `tmdb-state.json` | Snapshot đã fetch và xác minh, timestamp gốc, cursor, cooldown. |
| `consumer.sqlite3` | Audit theo topic/partition/offset, event identity và raw payload base64. |
| `*.lock` | flock ngăn hai process cùng sử dụng một state; khóa tự nhả khi process chết. |

Checkpoint ghi temp → fsync → atomic rename → fsync directory. File nguồn
phải bất biến trong một replay; thay checksum/target sẽ bị từ chối resume.
Binding target dùng bootstrap/topic, **không tự phát hiện cluster bị xóa và tạo
lại ở cùng địa chỉ**: khi dựng cluster mới phải dùng checkpoint/audit mới.
Đổi consumer group cũng phải dùng audit mới; audit cũ bị từ chối nếu sai group.

**At-least-once**, không hứa mỗi interaction chỉ xuất hiện một lần. Nếu Kafka
đã nhận nhưng process chết trước checkpoint, toàn bộ batch chưa checkpoint sẽ
được phát lại. Event_id giữ nguyên. Consumer SQLite duy trì identity trên disk
qua restart: `valid` là identity mới, `duplicate` là cùng ID/cùng nội dung nguồn,
`conflict` là cùng ID nhưng nội dung nguồn khác (không tính ingest_time).
Các trạng thái này loại trừ nhau trong thống kê mỗi lần consume.

Consumer lưu audit/dedup bằng transaction SQLite `synchronous=FULL`, sau đó
commit offset đồng bộ (`offset + 1`). Invalid/conflict được lưu raw và reason
trước khi commit; CLI exit 1 nếu gặp chúng. Nếu crash giữa hai bước, message có
thể đọc lại và được nhận diện duplicate; không mất record âm thầm. Đây là audit
validator local, không phải sink nghiệp vụ phân tán/exactly-once. Downstream
HDFS/Spark vẫn phải dedup theo event_id. Một audit dùng một process; mỗi consumer
worker trên nhiều máy cần thiết kế shared dedup nếu muốn thống kê toàn group.
SQLite tăng theo lượng dữ liệu và chưa tự xóa lịch sử; không xóa audit khi còn
cần dedup. Kafka retention có thể làm mất backlog chưa consume; theo dõi lag
và tăng retention khi chạy lâu. Offset gap được báo riêng, không tự kết luận
là mất dữ liệu vì Kafka offset có thể không liên tục.

SIGINT/SIGTERM ngừng enqueue, flush phần đã gửi, ghi cursor prefix đã ACK, exit
130. SIGKILL/mất điện dùng chính sách phát lại batch sau checkpoint. Nếu không
nhận đủ callback, checkpoint không tiến dù một phần batch đã đến broker.

Muốn **cố ý replay từ đầu**, chọn checkpoint mới, giữ audit consumer để thấy
trùng (không xóa checkpoint/audit cũ):

```bash
make -C modules/kafka_ingestion replay LIMIT=100 \
  ARGS='--checkpoint data/local/kafka_ingestion/replay-demo.json'
```

TMDB lưu snapshot vào outbox trước khi publish. Khi lỗi/restart, dùng lại
snapshot đó với event_id/time cũ, không fetch thay snapshot chưa hoàn thành.
Lần gọi lặp trong cooldown mặc định 600 giây không phát lại snapshot hoàn tất.
Hết interval mới fetch snapshot mới; cùng phim tại thời điểm snapshot mới là
sự kiện mới có chủ ý. Dùng cùng state cho mọi lần scheduler chạy.

Adapter Module 01 xử lý credential, TLS, JSON và fallback endpoint chính thức.
Module 02 retry lỗi kết nối/HTTP 5xx/429 tối đa `--retries` (mặc định 3), backoff
1/2/4 giây; 429 chờ ít nhất 60 giây, vì adapter chưa trả Retry-After. Không retry
401/403/TLS/schema lỗi. Mỗi fetch lấy một trang trending/day; adapter có tối đa
hai HTTP request nếu cần fallback host. Cooldown lưu cả lần fetch thất bại để
tránh restart loop gọi API liên tục. API key không đi theo redirect hoặc logs.

## Tests và đo hiệu năng

```bash
# Offline, gồm cả tests Module 01; integration được skip có chủ ý:
make -C modules/kafka_ingestion test

# Kafka thật tự khởi động riêng ở port ngẫu nhiên, data trong /tmp:
make -C modules/kafka_ingestion integration

# Bao gồm toàn bộ tests trong một lần (không skip integration):
RUN_KAFKA_INTEGRATION=1 .venv/bin/python -m pytest \
  modules/data_source/tests modules/kafka_ingestion/tests -q

# 200/1000/2000 ratings thật tại rate 10/100/500, đọc đồng thời:
make -C modules/kafka_ingestion benchmark
```

Integration cần `/opt/kafka` hoặc `KAFKA_HOME`, Java và input Module 01, không
cần TMDB key vì dùng snapshot thật đã lưu. Thiếu input/broker làm test fail,
không tự biến thành PASS. Broker test riêng được stop trong cleanup; test không
restart broker dev của bạn. Cần môi trường cho phép mở loopback socket.

Báo cáo producer ghi attempted/acknowledged/failed/unconfirmed và throughput;
consumer ghi valid/invalid/duplicate/conflict, offset regressions/gaps, committed
lag từng partition, độ trễ `consume_time - ingest_time`. Benchmark lấy CPU/RSS
Python+Kafka và RAM khả dụng WSL từ `/proc` mỗi 200 ms. Timestamp contract có độ
chính xác giây nên latency không phải phép đo mili giây chính xác. Không lấy
`now - event_time` lịch sử MovieLens làm độ trễ ingestion.

## Chẩn đoán lỗi

| Dấu hiệu | Xử lý |
| --- | --- |
| `delivery incomplete` / broker unavailable | `status`, kiểm tra broker logs; start broker rồi chạy lại cùng checkpoint. |
| Input/target changed | Kiểm tra có thay input/cluster hay không; dùng state mới chỉ khi chủ ý replay. |
| State already in use | Dừng process đang giữ state; không xóa lock để chạy đồng thời. |
| Consumer đọc 0 | Group đã commit hết; xem lag, hoặc dùng group **và audit** mới để đọc từ earliest còn được retention giữ. |
| Consumer invalid/conflict | Mở SQLite, xem bảng `messages` cột status/reason/payload, sửa nguồn/producer; không sửa contract riêng ở consumer. |
| TMDB cooldown | Snapshot hoàn tất hoặc fetch gần đây; chờ interval, không tạo state mới để vượt rate limit. |
| TMDB 401/403 | Kiểm tra API Key v3 trong root `.env`, không gửi key vào issue/log. |

Lifecycle overrides vẫn hỗ trợ `KAFKA_HOME`, `KAFKA_CONFIG`, `KAFKA_LOG_DIR`,
`KAFKA_LOCAL_BOOTSTRAP`, `KAFKA_HEAP_OPTS`, `KAFKA_START_TIMEOUT`,
`KAFKA_STOP_TIMEOUT`; xem hướng dẫn WSL. Sau khi Windows/WSL reboot, chạy
`make ... start`; không bật systemd autostart. Không commit broker logs/data,
secret, venv hay runtime state. Toàn bộ công việc đợt này chỉ nằm local, chưa push.
