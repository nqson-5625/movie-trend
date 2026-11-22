# Hướng dẫn sử dụng `modules/elasticsearch` (cho P1, P2, P3, P4, P5)

Tài liệu nhanh cách bật Elasticsearch trên k3s, cài Index Template cho từng lane, dùng `ESClient` để ghi/đọc an toàn và xuất `READY report` công bố Batch.

---

## 1. Khởi động & lệnh Makefile

Chạy từ thư mục gốc `movie-trend/`:

| Lệnh | Tác dụng | Ghi chú |
|---|---|---|
| `make -C modules/elasticsearch up` | Triển khai ES 8.15.3 lên k3s (`bigdata`) và chờ Ready | Chỉ cần chạy 1 lần khi chưa có pod `elasticsearch-0`. |
| `make -C modules/elasticsearch forward` | Mở cổng `127.0.0.1:9200` | Giữ chạy ở terminal riêng để code trên host gọi được ES. |
| `make -C modules/elasticsearch init-templates` | Cài 2 component template + toàn bộ `lanes/*/templates/*.json` | Chạy lại mỗi khi thêm/sửa template JSON. |
| `make -C modules/elasticsearch health` | Kiểm tra cluster health, version & templates | Lưu báo cáo ra `data/local/phase2/es_reports/health_check.json`. |
| `make -C modules/elasticsearch test` | Chạy unit test offline (`modules/elasticsearch/tests/common/`) | Thêm `test-live` để chạy cả test kết nối ES thật. |
| `make -C modules/elasticsearch status` / `logs` | Xem trạng thái pod hoặc log ES | Dùng `down` để tắt pod (giữ dữ liệu), `clean` để xóa cả PVC. |

Kiểm tra `.env` có `ES_HTTP_URL=http://127.0.0.1:9200` (nếu bỏ trống thì `ESClient` cũng mặc định dùng `http://127.0.0.1:9200`). Khi chạy script Python trực tiếp từ gốc repo, nhớ thêm `PYTHONPATH=.` trước `.venv/bin/python` (hoặc dùng `.venv/bin/python -m ...`).

---

## 2. Quyền ghi theo `owner` (`OWNER_WRITE_PATTERNS`)

`ESClient(owner=...)` tự động chặn ghi nhầm sang index của người khác:

| `owner` | Index/Alias được phép ghi (`bulk_upsert`) | Index được phép xóa (`delete_index`) |
|---|---|---|
| `"p1"` | *(không được ghi, chỉ đọc)* | Không |
| `"p2"` | `movie_batch_stats*`, `movie_daily_stats*`, `analytics_timeseries*`, `analytics_top_movies*`, `dev_p2_*` | Chỉ `dev_p2_*` |
| `"p3"` | `genre_trends*`, `analytics_genre_trends*`, `dev_p3_*` | Chỉ `dev_p3_*` |
| `"p4"` | `analytics_growth*`, `dev_p4_*` | Chỉ `dev_p4_*` |
| `"p5"` | `analytics_tmdb_signal*`, `dev_p5_*` | Chỉ `dev_p5_*` |
| `"spark_streaming"` | `movie_realtime_stats`, `genre_realtime`, `tmdb_realtime` | Không |
| `"admin"` | Toàn bộ (`*`) | Toàn bộ |

---

## 3. Code ví dụ các trường hợp cụ thể

### Trường hợp 1 — P2 / P3 / P4: Tạo & cài Index Template cho lane mình

Đặt file `<ten_bang>.json` trong thư mục lane của bạn (`modules/elasticsearch/lanes/<lane>/templates/`):
- **6 bảng Batch** (3 source + 3 analytics của P2, P3): dùng `"composed_of": ["movie_trend_base_settings", "movie_trend_batch_meta"]` *(riêng 3 source Batch thêm field `generated_at`; component `movie_trend_batch_meta` đã định nghĩa sẵn `batch_run_id` kiểu `keyword` và `batch_cutoff_time` kiểu `date` để P5 đối soát khi publish)*.
- **5 bảng Realtime** (của P4, P5): dùng `"composed_of": ["movie_trend_base_settings"]`.
- **Quy tắc đặt `index_patterns`:** ghi rõ từng prefix `dev_p2_...`, `dev_p3_...`, `dev_p4_...`, `dev_p5_...`, **tránh viết `dev_*_<ten_bang>*`** kẻo khớp nhầm sang bảng `analytics_*`.

Ví dụ file `modules/elasticsearch/lanes/genre_batch/templates/genre_trends.json` cho P3:

```json
{
  "index_patterns": [
    "genre_trends",
    "genre_trends_*",
    "dev_p2_genre_trends*",
    "dev_p3_genre_trends*",
    "dev_p4_genre_trends*",
    "dev_p5_genre_trends*"
  ],
  "priority": 200,
  "composed_of": [
    "movie_trend_base_settings",
    "movie_trend_batch_meta"
  ],
  "template": {
    "mappings": {
      "properties": {
        "date": { "type": "date", "format": "strict_date" },
        "genre": {
          "type": "keyword",
          "fields": { "text": { "type": "text" } }
        },
        "interaction_count": { "type": "long" },
        "rating_count": { "type": "long" },
        "rating_sum": { "type": "double" },
        "avg_rating": { "type": "double" },
        "generated_at": { "type": "date", "format": "strict_date_optional_time||strict_date" }
      }
    }
  }
}
```

Sau khi tạo file JSON xong, chạy lệnh cài và kiểm tra:

```bash
make -C modules/elasticsearch init-templates
make -C modules/elasticsearch health
```

---

### Trường hợp 2 — P2 / P3 / P4: Ghi hàng loạt bằng `ESClient.bulk_upsert` & bắt `BulkOperationError`

Dùng `serialize_key_v1` để tạo `_id` xác định và ghi qua `bulk_upsert`. Mặc định `raise_on_error=True` sẽ ném `BulkOperationError` ngay nếu có dù chỉ 1 dòng lỗi (kể cả khi ES trả HTTP 200).

```python
from modules.elasticsearch.common.config.es_client import BulkOperationError, ESClient
from modules.spark_sql.common.writers.key_serializer import serialize_key_v1

client = ESClient(owner="p2")

rows = [
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

records = []
for row in rows:
    doc = dict(row)
    doc["_id"] = serialize_key_v1((row["batch_run_id"], row["date"], row["movieId"]))
    records.append(doc)

try:
    result = client.bulk_upsert(
        index_name="dev_p2_movie_daily_stats",
        records=records,
        id_field="_id",
        batch_size=1000,
        refresh=True,
        raise_on_error=True,
    )
    print(f"Ghi OK {result.succeeded_count}/{result.total_sent} docs")
except BulkOperationError as exc:
    print(f"Có {exc.result.failed_count} dòng lỗi:")
    for err in exc.result.errors[:5]:
        print(err.doc_id, err.status, err.error_type, err.reason)

# Xóa index dev khi test xong (ESClient chỉ cho xóa dev_<owner>_*)
client.delete_index("dev_p2_movie_daily_stats")
```

---

### Trường hợp 3 — Đọc đủ 100% document bằng `search_all_pit`, đếm `count` & xem `get_aliases`

Không dùng `_search` phân trang thường (giới hạn 10.000 dòng); dùng `search_all_pit` để đọc nhất quán toàn bộ dữ liệu:

```python
from modules.elasticsearch.common.config.es_client import ESClient

reader = ESClient(owner="p1")  # Chỉ đọc, không có quyền ghi

if reader.index_exists("dev_p2_movie_daily_stats"):
    total = reader.count("dev_p2_movie_daily_stats")
    docs = reader.search_all_pit(
        index_or_alias="dev_p2_movie_daily_stats",
        page_size=5000,
        query={"term": {"movieId": 356}},  # Bỏ query nếu muốn lấy hết
        include_id=True,
    )
    print(f"Tổng={total}, đọc về={len(docs)}")

# Kiểm tra 1 alias hiện đang trỏ vào physical index nào
print(reader.get_aliases(["movie_batch_stats"]))
```

---

### Trường hợp 4 — P2 / P3: Xuất `READY report` sau khi build xong physical indices của run

Trước khi P5 công bố 6 Batch aliases, **P2** tạo `p2_ready_<RUN_ID>.json` (gồm 4 index: `movie_batch_stats`, `movie_daily_stats`, `analytics_timeseries`, `analytics_top_movies`) và **P3** tạo `p3_ready_<RUN_ID>.json` (gồm 2 index: `genre_trends`, `analytics_genre_trends`) trong `data/local/phase2/es_reports/`.

```python
from datetime import datetime, timezone
from modules.elasticsearch.common.publication.ready_report import (
    IndexReadyEntry,
    ReadyReport,
    save_ready_report,
)

run_id = "20261122_run01"
cutoff = "2018-09-26T14:59:10Z"

report = ReadyReport(
    owner="p3",
    schema_version="phase2-2.2",
    manifest_sha256="sha256_cua_batch_manifest",
    batch_run_id=run_id,
    batch_cutoff_time=cutoff,
    ref_version="ml-latest-small",
    sql_test_result="PASS",
    completed_at=datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    status="READY",
    indices=[
        IndexReadyEntry(
            alias_name="genre_trends",
            physical_index=f"genre_trends_{run_id}",
            row_count=32894,
            duplicate_id_count=0,
            bulk_error_count=0,
            sum_count=274480.0,
        ),
        IndexReadyEntry(
            alias_name="analytics_genre_trends",
            physical_index=f"analytics_genre_trends_{run_id}",
            row_count=1200,
            duplicate_id_count=0,
            bulk_error_count=0,
            sum_count=274480.0,
        ),
    ],
)

out = save_ready_report(report, f"data/local/phase2/es_reports/p3_ready_{run_id}.json")
print("Đã lưu READY report:", out)
```

---

### Trường hợp 5 — P5: Kiểm tra thử (`publish-dry`), công bố 6 Batch aliases (`publish`) và hoàn tác (`rollback`)

Khi P2 và P3 đã nộp đủ `p2_ready_<RUN_ID>.json` và `p3_ready_<RUN_ID>.json`:

```bash
# 1. Kiểm tra chéo 2 READY report + query thẳng 6 physical index trên ES (chưa đổi alias)
make -C modules/elasticsearch publish-dry RUN_ID=20261122_run01

# 2. Đổi đồng thời 6 Batch aliases trong 1 POST /_aliases & ghi publication_marker.json
#    (Nếu lỗi bất kỳ action nào sẽ tự rollback và khóa marker)
make -C modules/elasticsearch publish RUN_ID=20261122_run01

# 3. Hoàn tác về đúng bộ 6 physical index trước đó nếu cần
make -C modules/elasticsearch rollback RUN_ID=20261122_run01
```

---

## 4. Lỗi thường gặp

| Lỗi gặp phải | Nguyên nhân | Cách sửa |
|---|---|---|
| `requests.ConnectionError` | Chưa chạy ES hoặc chưa mở port-forward | Chạy `make -C modules/elasticsearch up` và `make -C modules/elasticsearch forward`. |
| `PermissionError` | Ghi sai index không thuộc `owner`, hoặc xóa index không bắt đầu bằng `dev_<owner>_` | Kiểm tra lại `owner` truyền vào `ESClient` và tên index đích. |
| `BulkOperationError` | Document thiếu/thừa field hoặc sai kiểu dữ liệu (vì `dynamic: strict`) | Kiểm tra lại Index Template đã `init-templates` chưa và đối chiếu tên/kiểu cột với `DATA_CONTRACT_Phase2.md`. |
| `ValueError` khi `bulk_upsert` | Thiếu `_id`, tên index không hợp lệ, hoặc số chứa `NaN`/`Infinity` | Đảm bảo mỗi record có `_id` hợp lệ từ `serialize_key_v1` và không có `NaN`/`Inf`. |
| `ValueError` / `PublicationError` khi `publish` | Thiếu 1 trong 6 physical index, lệch `batch_run_id`/`batch_cutoff_time`/`ref_version`/`manifest_sha256`, hoặc còn `bulk_error_count > 0` | Đối chiếu lại 2 file READY report P2/P3 và dữ liệu thực tế trên 6 physical index trước khi `publish` lại. |
