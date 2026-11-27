# Lane `genre_batch` (P3) — schema và fixture

| | |
| --- | --- |
| Contract | `DATA_CONTRACT_Phase2.md` bản **Phase2-3.0** (mục 4, 5, 6.1, 9); `schema_version = phase2-3.0` |
| Owner | P3 — Giang |
| Consumer | P3 (D3: `analytics_genre_trends` -> `dv_d3_genre_trends`) |
| Fixture `generated_at` | `2026-11-27T00:00:00Z` |
| Nhãn | **FIXTURE** — dữ liệu dựng tay để phát triển song song, **không phải** kết quả MapReduce thật. `batch_run_id = fixture-p3-v1` |

Index template tương ứng nằm ở `modules/elasticsearch/lanes/genre_batch/templates/`.

## Hai bảng của lane

| Bảng | Loại | Grain / `_id` (`serialize_key_v1`) | Nguồn |
| --- | --- | --- | --- |
| `genre_trends` | source | `(batch_run_id, date, genre)` | `genre_daily_stats.tsv` của Module 04 (giữ `UNKNOWN`) |
| `analytics_genre_trends` | analytics | `(batch_run_id, period_type, period_start, genre)` | Spark SQL từ `genre_trends` |

- `analytics_genre_trends`: `period_type` ∈ `day`/`month`/`year`, `period_start` là đầu kỳ UTC.
- `rank_in_period`: `row_number` theo `interaction_count` giảm dần, hòa thì `genre` tăng dần theo thứ tự từ điển.
- **Multi-label semantics:** Một phim có thể thuộc nhiều thể loại nên lượt gắn genre của các thể loại cộng lại **không phải** tổng lượt rating hệ thống.
- **Weighted average:** `avg_rating = rating_sum / rating_count` tính trên đúng grain (không dùng average-of-averages).
- Khi tạo `_id`, `date`/`period_start` phải là kiểu `date` (không phải chuỗi) để khớp writer Spark dùng chung.

## Thư mục

```text
contracts/phase2/genre_batch/
├── README.md
├── build_fixtures.py        sinh fixtures/ từ bảng ngày viết tay (nguồn sự thật duy nhất)
├── schemas/                 JSON Schema Draft 2020-12 cho từng bảng (additionalProperties=false)
└── fixtures/
    ├── manifest.json        manifest giả lập; manifest_hash = sha256 của file này
    ├── genre_trends.jsonl           11 dòng (4 ngày, 4 thể loại)
    └── analytics_genre_trends.jsonl 27 dòng (11 day + 10 month + 6 year)
```

## Fixture phủ những ca nào (contract mục 9, P3)

Dữ liệu gốc: 4 thể loại (`Action`, `Comedy`, `Drama`, `UNKNOWN`), 4 ngày, **16 lượt gắn genre, tổng điểm 69,5**, mốc T = `2018-03-01T00:00:00Z`.

| Ca bắt buộc | Có trong fixture |
| --- | --- |
| >= 2 thể loại và >= 2 kỳ | 4 thể loại (`Action`, `Comedy`, `Drama`, `UNKNOWN`), 4 ngày, 2 tháng (`2017-12`, `2018-01`), 2 năm (`2017`, `2018`) |
| Thể loại `UNKNOWN` | Xuất hiện ở ngày `2017-12-31` và `2018-01-05` |
| Multi-label | Mô phỏng 12 rating có phim đa thể loại `Action\|Drama` tạo ra 16 lượt gắn genre |
| Ca hòa hạng (tie-break) | Tháng `2017-12`: `Action` và `Drama` cùng 3 lượt -> `Action` hạng 1, `Drama` hạng 2; năm `2018`: `Action` (3) hạng 1, `Comedy`/`Drama` cùng 2 lượt -> `Comedy` hạng 2, `Drama` hạng 3 |
| Kỳ trống trước cutoff | Tháng `2018-02` (trước T) không có dữ liệu |

## Lệnh

```bash
.venv/bin/python contracts/phase2/genre_batch/build_fixtures.py           # sinh lại fixtures/
.venv/bin/python contracts/phase2/genre_batch/build_fixtures.py --check   # kiểm fixtures/ khớp script
.venv/bin/python -m pytest modules/elasticsearch/tests/lanes/test_genre_batch_lane.py -q
```

### Nạp fixture vào Elasticsearch local (để dựng/thử D3)

Cần ES đang chạy theo `modules/elasticsearch/Huong_dan.md` (`make up`, `make forward`).

```bash
PYTHONPATH=. make -C modules/elasticsearch init-templates
.venv/bin/python -m modules.elasticsearch.lanes.genre_batch.load_fixtures --replace
```

Kết quả: `dev_p3_genre_trends` 11 document, `dev_p3_analytics_genre_trends` 24 document.

## Số đối chứng với dữ liệu thật (run `20261024T104145Z`, T = `2018-10-01T00:00:00Z`)

| Bảng | Số lượng kỳ vọng |
| --- | --- |
| `genre_trends` (`genre_daily_stats.tsv`) | **32.894** dòng genre-day |
| Tổng lượt gắn genre | **274.480** lượt gắn genre |
