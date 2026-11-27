# Lane `movielens_realtime` (P4) — schema và fixture K0

| | |
| --- | --- |
| Contract | `DATA_CONTRACT_Phase2.md` bản **Phase2-3.0** (mục 3, 4, 5, 6, 6.2, 9, 11.1, 12); `schema_version = phase2-3.0` |
| Owner | P4 — Hiếu |
| Consumer | P4 (D4: `dv_d4_growth_current` không time field, `dv_d4_growth_history` time field `window_start`); P5 (publisher `analytics_growth_current`); P2 (query 8 ở K2) |
| Fixture `generated_at` | `2026-11-26T00:00:00Z` |
| Nhãn | **FIXTURE** — rating dựng tay để phát triển song song, **không phải** replay MovieLens thật. Mọi `analytics_run_id`/`snapshot_version` bắt đầu bằng `fixture-p4-` |

Index template tương ứng nằm ở `modules/elasticsearch/lanes/movielens_realtime/templates/`.

## Bốn bảng của lane

| Bảng | Loại | Grain / `_id` | Writer |
| --- | --- | --- | --- |
| `movie_realtime_stats` | source | `(movieId, window_start, window_size)`; `_id` literal `movieId\|window_start\|window_size` | Spark Streaming Module 05 (P4 nghiệm thu) |
| `genre_realtime` | source | `(genre, window_start, window_size)`; `_id` literal `genre\|window_start\|window_size` | Spark Streaming Module 05 (P4 nghiệm thu) |
| `analytics_growth` | analytics lịch sử | `(window_size, window_start, entity_type, entity_key)`; `_id = serialize_key_v1` | P4 Spark SQL, upsert theo full key |
| `analytics_growth_current` | analytics current | `(snapshot_version, window_size, entity_type, entity_key)`; physical `_id = serialize_key_v1(window_size, entity_type, entity_key)` | P4 build physical `analytics_growth_current-v<snapshot_version>`; P5 swap alias |

- Source template đúng **bằng** tập cột Spark Streaming ghi (`es_sink` bỏ `doc_id`) và schema `rt_movie`/`rt_genre`
  của reader dùng chung; mapping `dynamic=strict` nên thừa/thiếu field đều làm Bulk lỗi.
- `trending_score = (current_window_count - previous_window_count) / (previous_window_count + 1)`; previous là **đúng**
  `window_start - window_size` cùng entity/size, không phải window gần nhất.
- Thiếu previous: `previous_window_present=false`, `previous_window_count=null`, `trending_score=null` (không đổi thành 0).
- `title` chỉ là nhãn, lấy từ reference (`title_source = movie_reference/<ref_version>`); không có trong reference thì
  `title = title_source = null`, D4 hiển thị `entity_key`. Dòng genre luôn `movieId = title = null`.
- `analytics_growth_current` = toàn bộ business field của history tại `W_s` + `snapshot_version`,
  `selected_window_start (= window_start)`, `source_complete_through`, `history_analytics_run_id`.
- Không có field `is_latest_window` (contract 2.1 và 6.2).
- `timeline_mode = replay_history` (giá trị dùng trong ví dụ của `modules/spark_sql/Huong_dan.md`; contract chưa liệt kê enum).

Pattern template: history dùng `analytics_growth`, `analytics_growth-*` và **không** dùng `analytics_growth*`. Lý do:
pattern đó cũng khớp `analytics_growth_current*`, và ES từ chối hai template cùng priority 200 khớp chung một tên index.
Physical names khớp role `movie_trend_spark_sql_p4` trong `common/roles/phase2_roles.json`.

## Thư mục

```text
contracts/phase2/movielens_realtime/
├── README.md
├── build_fixtures.py        sinh fixtures/ từ EVENTS + REFERENCE viết tay (nguồn sự thật duy nhất)
├── schemas/                 JSON Schema Draft 2020-12 cho 4 bảng (additionalProperties=false)
└── fixtures/
    ├── snapshots.json                                   history generation + mốc complete của từng snapshot đã publish
    ├── movie_realtime_stats.jsonl                       32 dòng
    ├── genre_realtime.jsonl                             49 dòng
    ├── analytics_growth.jsonl                           81 dòng (= 32 movie + 49 genre)
    ├── analytics_growth_current.fixture-p4-s001.jsonl   15 dòng  READY
    └── analytics_growth_current.fixture-p4-s002.jsonl   22 dòng  READY (version mới nhất)
```

## Fixture phủ những ca nào (contract mục 9, P4)

Dữ liệu gốc là 23 rating ngày 2018-09-20 (trước T = `2018-10-01T00:00:00Z`, đúng bản chất replay) trên 5 phim:
218, 222, 227, 343 (title/genre lấy từ `contracts/samples/movie-reference.sample.tsv`, ref `20261010-01e56f1d`)
và 999001 (cố ý không có trong reference).

| Artefact | `analytics_run_id` / snapshot | `source_complete_through` | W 10m | W 1h | `published_at` |
| --- | --- | --- | --- | --- | --- |
| Source (Spark Streaming) | — | — | — | — | `updated_at` `2026-11-26T00:00:00Z` |
| History `analytics_growth` | `fixture-p4-history-g1` | — | — | — | `2026-11-26T00:30:00Z` |
| Current | `fixture-p4-s001` (history g1) | `11:50Z` | `11:40` | `10:00` | `2026-11-26T01:00:00Z` |
| Current | `fixture-p4-s002` (history g1) | `12:00Z` | `11:50` | `11:00` | `2026-11-26T02:00:00Z` |

Lifecycle: source → history g1 → s001 → s002. Cả hai snapshot tham chiếu `history_analytics_run_id` là generation có
thật trong fixture, và generation đó được publish **trước** snapshot nên không có dữ liệu "từ tương lai".
Fixture chỉ lưu snapshot đã READY/publish. Các candidate lỗi (thiếu size 1h, thiếu entity type, thiếu một entity,
trùng khóa, chọn window chưa complete, business field lệch history, `selected_window_start` sai) được **dựng ngay trong
test** từ s002 và phải bị READY check từ chối; chúng không có `published_at` và không lưu thành file.

| Ca bắt buộc | Có trong fixture |
| --- | --- |
| Hai size | 10m và 1h; mỗi size cộng đủ 23 rating / 39 genre assignment |
| Window cũ / được chọn / chưa complete | 10m: 11:30 cũ, 11:50 chọn, 12:00 chưa complete. 1h: 09:00 cũ, 11:00 chọn, 12:00 chưa complete. Window 12:00 có trong source và history nhưng không vào current |
| Movie + genre | Mọi snapshot READY có cả `entity_type=movie` và `genre` ở cả hai size |
| Previous có | Ví dụ 10m s002: phim 222 current 3, previous 1, score 1,0; phim 218 current 1, previous 2, score −0,333 |
| Score = 0 khác null | Phim 227 10m s002 và phim 343 1h s002: current = previous nên score `0.0` |
| Previous thiếu | Phim 999001 10m có window 11:30 nhưng thiếu 11:40; phim 227 1h có 09:00 nhưng thiếu 10:00. Cả hai đều `present=false`, score `null` (không lấy window gần nhất) |
| UNKNOWN / thiếu title | Phim 999001 → genre `UNKNOWN`, `title=null` |
| Hai physical current versions | s001 và s002 trỏ window khác nhau nên rollback thấy rõ; `_id` của cùng entity giữ nguyên giữa các version |

**Kỳ trước thiếu = null, không có ca count = 0.** Source canonical chỉ sinh window có ít nhất 1 rating (Spark
Streaming groupBy; reader `rt_movie`/`rt_genre` khai báo `avg_rating` non-null). Vì vậy kỳ trước không có rating chính
là kỳ thiếu, với `present=false`, `previous_window_count=null` và `trending_score=null`. Schema analytics đặt
`previous_window_count >= 1` khi present, và test chặn mọi trường hợp tự đổi null thành 0. Câu "previous tồn tại và
count thật bằng 0" ở contract mục 5 không xảy ra được với mô hình dữ liệu hiện tại; cần P1 ghi change request vào
`DATA_CONTRACT_Phase2.md` mục 5/9 (lane P4 không tự sửa file contract).

## Lệnh

```bash
.venv/bin/pip install -r modules/spark_sql/requirements.txt                      # cần cho test so khớp reader/writer Spark
.venv/bin/python contracts/phase2/movielens_realtime/build_fixtures.py           # sinh lại fixtures/
.venv/bin/python contracts/phase2/movielens_realtime/build_fixtures.py --check   # kiểm fixtures/ khớp script
.venv/bin/python -m pytest modules/elasticsearch/tests/lanes -q                  # template + schema + fixture
```

### Nạp fixture vào Elasticsearch local (để dựng/thử D4)

Cần ES đang chạy theo `modules/elasticsearch/Huong_dan.md` (Secret + `.env` có `ES_*`, `make up`, `make forward`).

```bash
PYTHONPATH=. make -C modules/elasticsearch init-templates
.venv/bin/python -m modules.elasticsearch.lanes.movielens_realtime.load_fixtures --replace
```

Kết quả: `dev_p4_movie_realtime_stats` 32, `dev_p4_genre_realtime` 49, `dev_p4_analytics_growth` 81,
`dev_p4_analytics_growth_current-vfixture-p4-s001` 15, `dev_p4_analytics_growth_current-vfixture-p4-s002` 22 document.
Loader từ chối ghi nếu template chưa được cài. Chạy lại không nhân đôi document vì ghi đè theo `_id`.

Loader **không** tạo alias: `ESClient.update_aliases` chỉ cho owner `admin`. Nếu cần alias dev để dựng D4 thì P5 trỏ
`dev_p4_analytics_growth_current` → `dev_p4_analytics_growth_current-vfixture-p4-s002`.

Truy vấn mẫu đúng cách D4 đọc:

| Panel D4 | Index | Lọc / aggregation |
| --- | --- | --- |
| Cửa sổ đã publish | current | đúng một `window_size`; `max(selected_window_start)` |
| Phim / thể loại biến động | current | đúng một `window_size` + `entity_type`; terms `entity_key`, sort `trending_score`; hiện previous/present |
| Tra cứu lịch sử | `dev_p4_analytics_growth` | đúng một `window_size`; date histogram `window_start` với **absolute** range (vd. 2018-09-20), không dùng "Last 15 minutes" |

## Số đối chứng với dữ liệu thật

| Bảng | Số dòng kỳ vọng |
| --- | --- |
| `movie_realtime_stats` | 201.627 (100.826 docs 10m + 100.801 docs 1h; mỗi size cộng 100.836 rating) |
| `genre_realtime` | 127.770 (77.500 docs 10m + 50.270 docs 1h; mỗi size cộng 274.480 genre assignment) |
| `analytics_growth` | 329.397 (= 201.627 + 127.770, một dòng cho mỗi source window) |
