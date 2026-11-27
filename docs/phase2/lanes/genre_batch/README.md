# Runbook Lane P3 — `genre_batch` & Dashboard D3

**Owner:** P3 — Giang  
**Contract:** `DATA_CONTRACT_Phase2.md` (Phase2-3.0)  
**Tables:** `genre_trends` (source) -> `analytics_genre_trends` (analytics) -> `dv_d3_genre_trends` -> Dashboard **D3**

---

## 1. Quy chuẩn nghiệp vụ bắt buộc (P3)

1. **Giữ `UNKNOWN`:** Không loại bỏ bản ghi có `genre = "UNKNOWN"`.
2. **Đa nhãn (Multi-label):** Một phim có nhiều thể loại sẽ được đếm ở từng thể loại. Tổng lượt gắn genre (**274.480** ở dữ liệu thật) **không phải** tổng số rating của hệ thống.
3. **Weighted average:** `avg_rating = SUM(rating_sum) / SUM(rating_count)`, không lấy average-of-averages.
4. **Thứ hạng:** `rank_in_period` = `ROW_NUMBER()` theo `interaction_count` giảm dần, hòa thì `genre` tăng dần theo thứ tự từ điển.
5. **Atomic Batch Publish:** P3 chỉ xuất `p3_ready_<RUN_ID>.json`; P5 dùng cặp `p2_ready` + `p3_ready` để chuyển đồng thời **6 Batch aliases** trong một request.

---

## 2. Quy trình chạy từng bước

### Bước 0 (K0): Kiểm tra offline & cài Index Templates
```bash
cd /mnt/d/2026.1/LTVXLDLL/BTL
set -a && source .env && set +a

# Chạy toàn bộ unit tests offline của P3 (ES lane + Spark SQL + Kibana D3)
/home/giang/movie-trend/.venv/bin/python -m pytest -v \
  modules/elasticsearch/tests/lanes/test_genre_batch_lane.py \
  modules/spark_sql/tests/lanes/test_genre_batch_sql.py \
  modules/kibana/tests/test_d03_genres_saved_objects.py

# Cài templates lên Elasticsearch (đang chạy port-forward 9200)
PYTHONPATH=. make -C modules/elasticsearch init-templates PYTHON=/home/giang/movie-trend/.venv/bin/python
```

### Bước 1 (K1): Nạp dữ liệu thật `genre_daily_stats.tsv` lên physical source index
```bash
# 1. Dry-run kiểm tra contract trước khi ghi ES (không cần ES)
/home/giang/movie-trend/.venv/bin/python -m modules.elasticsearch.lanes.genre_batch.load_source \
  --input data/batch/genre_daily_stats.tsv \
  --manifest data/batch/manifest.json \
  --dry-run

# 2. Ghi lên physical index (mặc định genre_trends_<run_id>, đối chứng 32.894 dòng / 274.480 lượt gắn)
/home/giang/movie-trend/.venv/bin/python -m modules.elasticsearch.lanes.genre_batch.load_source \
  --input data/batch/genre_daily_stats.tsv \
  --manifest data/batch/manifest.json
```

### Bước 2 (K2): Chạy Spark SQL tạo `analytics_genre_trends_<run_id>`
```bash
/home/giang/movie-trend/.venv/bin/python -m modules.spark_sql.lanes.genre_batch.build_genre_trends \
  --source-index genre_trends_20261024t104145z \
  --target-index analytics_genre_trends_20261024t104145z \
  --ref-version <REF_VERSION> \
  --manifest-hash <MANIFEST_SHA256> \
  --analytics-run-id 20261024t104145z-analytics \
  --io-mode fixture
```

### Bước 3 (K2P): Xuất `p3_ready_<RUN_ID>.json` để P5 atomic publish
```bash
/home/giang/movie-trend/.venv/bin/python -m modules.elasticsearch.lanes.genre_batch.emit_ready_report \
  --run-id 20261024t104145z \
  --cutoff 2018-10-01T00:00:00Z \
  --ref-version <REF_VERSION> \
  --manifest-sha256 <MANIFEST_SHA256> \
  --source-index genre_trends_20261024t104145z \
  --source-rows 32894 \
  --source-sum 274480 \
  --analytics-index analytics_genre_trends_20261024t104145z \
  --analytics-rows <ANALYTICS_ROW_COUNT>
```
Sau khi P2 nộp `p2_ready_<RUN_ID>.json`, P5 chạy:
```bash
make -C modules/elasticsearch publish-dry RUN_ID=20261024t104145z
make -C modules/elasticsearch publish RUN_ID=20261024t104145z
# Nếu có sự cố cần hoàn tác:
make -C modules/elasticsearch rollback RUN_ID=20261024t104145z
```

### Bước 4 (K3 & K4): Import Dashboard D3 & Smoke test sau publish
```bash
# Import D3 vào Kibana Space integration
curl -X POST "http://127.0.0.1:5601/s/integration/api/saved_objects/_import?overwrite=true" \
  -u "$ES_USERNAME:$ES_PASSWORD" \
  -H "kbn-xsrf: true" \
  --form file=@modules/kibana/saved_objects/owners/p3/d03_genres.ndjson

# Kiểm tra alias sau publish
curl -s -u "$ES_USERNAME:$ES_PASSWORD" "$ES_HTTP_URL/_alias/genre_trends,analytics_genre_trends"
```
