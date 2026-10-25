# Module 04 — Hadoop MapReduce (Batch Layer)

**Owner:** Đỗ Sơn · **Trạng thái:** 3 job chạy đúng trên YARN (k3s), khớp pandas 100%; run `20261024T104145Z` đã publish kèm manifest.

Module 04 tính các số liệu **lịch sử, chính xác** từ toàn bộ ratings thô trên HDFS bằng
Hadoop MapReduce chạy trên YARN, và ghi ba Batch View cho Module 06 nạp vào Elasticsearch.
Tuân thủ [DATA_CONTRACT.md](../../DATA_CONTRACT.md) mục 10 và quy chuẩn HDFS của Module 03
([HDFS_DATA_INTAKE_GUIDE.md](../hdfs/HDFS_DATA_INTAKE_GUIDE.md),
[BAO_CAO_MODULE_03_HDFS.md](../hdfs/BAO_CAO_MODULE_03_HDFS.md)).

```
Kafka ─▶ Sink (M3) ─▶ HDFS /raw/ratings ──┐
                     HDFS /movies/ref ────┴─▶ MapReduce (M4) ─▶ HDFS /batch/* ─▶ Batch Loader (M6)
```

## Input

| Dữ liệu | Đường dẫn HDFS | Trường dùng |
| --- | --- | --- |
| Ratings thô (JSON Lines) | `/raw/ratings/dt=YYYY-MM-DD/*.jsonl` | `event_id`, `movieId`, `rating`, `event_time` |
| Reference (TSV, header) | `/movies/ref/current/movie_reference.tsv` | `movieId`, `title`, `genres` |

## Output

Ghi vào `/batch/<bảng>/run_id=<run_id>/`:

| Bảng | Khóa | Cột |
| --- | --- | --- |
| `movie_stats` | movieId | movieId, title, total_count, rating_count, rating_sum, avg_rating, batch_run_id, batch_cutoff_time, generated_at |
| `movie_daily_stats` | date, movieId | date, movieId, interaction_count, rating_count, rating_sum, avg_rating, batch_run_id, batch_cutoff_time, generated_at |
| `genre_daily_stats` | date, genre | date, genre, interaction_count, rating_count, rating_sum, avg_rating, batch_run_id, batch_cutoff_time, generated_at |

`avg_rating = rating_sum / rating_count`. Hiện ghi **TSV không header** (`part-NNNNN`), thứ tự cột nằm trong manifest;
**đang chờ chốt với Module 06** (sổ tay Module 03 gợi ý `part-r-*.csv`).

## Quy tắc

- Chỉ tính event có `event_time < batch_cutoff_time` (T tròn giờ, UTC) — phần sau T thuộc Realtime View (contract 5.4).
- `date` là ngày UTC của `event_time`, không dùng `ingest_time`.
- Dedup theo `event_id` trước khi cộng (contract 16.2), vì sink hiện chưa loại trùng.
- Thể loại: `movieId` không có trong reference, genres rỗng hoặc `(no genres listed)` ⇒ `UNKNOWN`,
  giống Module 05, để Module 07 ghép Batch + Realtime đúng nhóm.
- Bản ghi sai contract bị bỏ và **được đếm** bằng Hadoop counter, không bỏ im lặng.

## Cách chạy

Điều kiện: k3s + HDFS của Module 03 đang chạy (`bash modules/hdfs/run.sh start`) và đã có dữ liệu trong `/raw/ratings`.
Khi chạy batch nên tắt Kafka để đủ RAM (YARN cần thêm ~1 GB khi rảnh, tối đa ~2,6 GB khi chạy job).

```bash
make -C modules/mapreduce test                       # test offline: pipe mapper|sort|reducer, đối chiếu pandas
make -C modules/mapreduce yarn-up                    # bật YARN: 1 ResourceManager + 2 NodeManager
make -C modules/mapreduce python-archive             # 1 lần: đưa Python 3 portable lên HDFS /apps/python/
make -C modules/mapreduce batch CUTOFF=2018-10-01T00:00:00Z            # chạy 3 job, cùng một run_id
make -C modules/mapreduce batch CUTOFF=... JOBS="movie_stats"          # chỉ chạy một job
CUTOFF=... BATCH_ROOT=/tmp/m4-experiments bash modules/mapreduce/scripts/run_batch.sh   # chạy thử, không ghi /batch
make -C modules/mapreduce yarn-down                  # tắt YARN để trả RAM (giữ cấu hình)
```

Mỗi lần chạy `batch`:
1. Gửi job Hadoop Streaming từ pod `yarn-resourcemanager-0` cho từng bảng, cùng một `run_id` (`YYYYMMDDTHHMMSSZ`).
2. Kiểm tra counters; nếu bất thường (ví dụ mọi bản ghi bị loại) thì **dừng, không publish**.
3. Ghi `/batch/_meta/run_id=<run_id>/manifest.json` **sau cùng** (contract 9.3): cutoff, ref_version, thứ tự cột,
   counters chất lượng dữ liệu. **Module 06 chỉ nên đọc run đã có manifest.** Một `run_id` không publish lại (contract 16.4).

Log của Hadoop client nằm ở `data/local/mapreduce/run_id=<run_id>/` (gitignore). Log của từng container:
`kubectl exec -n bigdata yarn-resourcemanager-0 -c resourcemanager -- yarn logs -applicationId <application_id>`.

## Triển khai

- Hadoop Streaming trên YARN trong k3s (namespace `bigdata`, image `apache/hadoop:3.3.6`), không Docker.
- YARN: 1 ResourceManager + 2 NodeManager (`k8s/`), dùng lại `core-site.xml`/`hdfs-site.xml` từ ConfigMap `hdfs-config`
  của Module 03 (chỉ đọc). Bộ nhớ: mỗi NodeManager 768 MB cho container; ApplicationMaster 384 MB; map/reduce 256 MB.
- Reference gửi tới các node qua distributed cache (`-files hdfs:///movies/ref/current/movie_reference.tsv#movie_reference.tsv`).
- Gộp file nhỏ: sink tạo hàng nghìn file nhỏ nên dùng `CombineTextInputFormat` (split tối đa 8 MB) kèm
  `stream.map.input.ignoreKey=true` (thiếu option này mapper nhận `offset TAB json` và loại mọi bản ghi).

```text
modules/mapreduce/
├── src/          mr_common.py + mapper/reducer của 3 job (Python 3, chỉ thư viện chuẩn)
├── tests/        test_jobs.py — trường hợp đặc biệt + toàn bộ dữ liệu thật, đối chiếu pandas
├── k8s/          01-yarn-config, 02-resourcemanager, 03-nodemanager
├── scripts/      run_batch.sh, build_manifest.py, upload_python.sh
└── Makefile
```

## Lưu ý cho nhóm

- Image `apache/hadoop:3.3.6` là CentOS 7.6, Java 8, **chỉ có Python 2.7, không có `python3`**
  (đã kiểm tra danh sách file của image ngày 23/10/2026). Lệnh mẫu `-mapper "python3 mapper.py"`
  sẽ lỗi trên image này. **Module 04 chọn:** viết mapper/reducer bằng Python 3 (thư viện chuẩn) và gửi kèm
  một bản Python 3 portable tới các NodeManager qua distributed cache (`-archives`), không đổi image.

## Việc đang mở

| Việc | Trạng thái |
| --- | --- |
| Chọn cách chạy mapper/reducer (do image không có python3) | Đã chốt: Python 3 qua `-archives` |
| Cài k3s + HDFS của Module 03 trên máy dev | Xong (24/10): 2 DataNode, đủ 100.836 event_id trên `/raw/ratings` |
| Dựng YARN (1 RM + 2 NM) | Xong (24/10): `modules/mapreduce/k8s/`, smoke test wordcount chạy trên 2 NodeManager |
| Job 1 `movie_stats` | Xong: 9.724 phim, loại 1.000 bản trùng |
| Job 2 `movie_daily_stats` | Xong: 99.538 dòng (ngày, phim) |
| Job 3 `genre_daily_stats` | Xong: 32.894 dòng (ngày, thể loại), có `UNKNOWN` |
| Script chạy batch + manifest (`make batch`) | Xong: run `20261024T104145Z`, cả 3 bảng khớp pandas 100% |
| Thử phân tán / chịu lỗi | Xong (25/10): 1 vs 2 NodeManager; giết NodeManager và tắt DataNode giữa job ⇒ job vẫn xong, output giống hệt run chuẩn |
| Chốt định dạng output với Module 06 | Chưa chốt |
