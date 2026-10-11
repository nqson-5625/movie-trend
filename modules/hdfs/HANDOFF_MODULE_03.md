# Đặc tả Bàn giao & Nghiệm thu (Handoff Specification) — Module 03 HDFS

Tài liệu này xác lập quy chuẩn bàn giao (Handoff) và tiêu chí nghiệm thu kỹ thuật giữa **Module 03 (HDFS - Giang)** với các Module khác trong dự án **Movie-Trend**.

---

## 1. Ma trận trách nhiệm giao tiếp (Interface RACI Matrix)

| Giao tiếp giữa các Module | Module 03 (HDFS) | Bên đối tác | Nội dung bàn giao | Trạng thái hiện tại |
| :--- | :---: | :---: | :--- | :---: |
| **Module 01 $\rightarrow$ Module 03** | **C** (Consulted) / **I** (Informed) | **Module 01** (**A**ccountable / **R**esponsible) | **Bước 16**: Cung cấp HDFS endpoint; tải `movie_reference.tsv`, verify SHA256 & số dòng, publish `/movies/ref/current/`. | **Sẵn sàng** (Chờ chạy script) |
| **Module 02 $\rightarrow$ Module 03** | **R** (Responsible) | **Module 02** (**A**ccountable) | HDFS Sink: Kéo message từ `movie-events` và `tmdb-snapshots`, partition theo `dt=YYYY-MM-DD` của `event_time`. | **Sẵn sàng** (Chờ Kafka phát sự kiện) |
| **Module 03 $\rightarrow$ Module 04** | **A**ccountable | **Module 04** (Đỗ Sơn - **R**) | Cung cấp dữ liệu thô `/raw/ratings` + canonical reference; mở quyền ghi `/batch/` cho MapReduce. | **Sẵn sàng** |
| **Module 03 $\rightarrow$ Module 05** | **A**ccountable | **Module 05** (Thắng/Hiếu - **R**) | Cung cấp `/movies/ref/current/movie_reference.tsv` cho static join. | **Sẵn sàng** |
| **Module 03 $\rightarrow$ Module 09** | **R**esponsible | **Module 09** (**A**ccountable) | Cung cấp báo cáo tài nguyên (< 1.5 GB RAM), logs, và kết quả kiểm thử e2e. | **Sẵn sàng** |

---

## 2. Chi tiết bàn giao từng giao tiếp

### 2.1. Handoff với Module 01 (Data Source & Preparation) — Bước 16
- **Vấn đề đã giải quyết:** Trong [modules/data_source/README.md](../data_source/README.md) nêu rõ: *"Bước 16: Chờ HDFS endpoint/path để publish reference"*. Module 03 đã cung cấp trọn vẹn giải pháp này.
- **Quy trình nghiệm thu 4 bước:**
  1. Module 03 mở port WebHDFS `http://localhost:9870/webhdfs/v1` và RPC `hdfs://localhost:9000`.
  2. File `data/prepared/reference/movie_reference.tsv` được tải lên `/movies/ref/version=<ref_version>/movie_reference.tsv`.
  3. Kiểm tra tính toàn vẹn:
     - Số dòng phải là: **9.743 dòng** (9.742 phim + 1 header).
     - Checksum SHA256 trên HDFS khớp 100% với file local manifest.
  4. Cập nhật atomic sang canonical path: `/movies/ref/current/movie_reference.tsv`.
- **Lệnh thực hiện:**
  ```bash
  python modules/hdfs/scripts/publish_reference.py
  ```

---

### 2.2. Handoff với Module 02 (Kafka / Ingestion)
- **Quy cách tiếp nhận:**
  - Topic `movie-events` $\rightarrow$ `/raw/ratings/dt=YYYY-MM-DD/part-*.jsonl`
  - Topic `tmdb-snapshots` $\rightarrow$ `/raw/tmdb/dt=YYYY-MM-DD/part-*.jsonl`
- **Yêu cầu kỹ thuật:**
  - Định dạng: JSON Lines UTF-8, mỗi dòng 1 JSON object độc lập.
  - Phân vùng: Lấy ngày từ trường `event_time` (UTC ISO 8601), không lấy ngày ingest.
  - Xử lý bản ghi hỏng (Bad records / Schema violation): Đẩy vào `/quarantine/<source>/dt=YYYY-MM-DD/error-*.jsonl` kèm trường `error_reason`.
- **Lệnh thực hiện daemon:**
  ```bash
  python modules/hdfs/scripts/kafka_hdfs_sink.py
  ```

---

### 2.3. Handoff với Module 04 (Hadoop MapReduce - Đỗ Sơn)
- **Dữ liệu đầu vào (Input):**
  - Ratings: `hdfs://namenode:9000/raw/ratings/dt=*/part-*.jsonl`
  - Canonical Reference: `hdfs://namenode:9000/movies/ref/current/movie_reference.tsv`
- **Dữ liệu đầu ra (Output):**
  - Thư mục `/batch/` đã được mở quyền `chmod 777`.
  - Cấu trúc thư mục con cho từng batch job:
    * `/batch/movie_stats/run_id=<run_id>/`
    * `/batch/movie_daily_stats/run_id=<run_id>/`
    * `/batch/genre_daily_stats/run_id=<run_id>/`
    * `/batch/_meta/run_id=<run_id>/manifest.json`
- **Lệnh kiểm tra:**
  ```bash
  bash modules/hdfs/scripts/verify_mapreduce_handoff.sh
  ```

---

## 3. Biên bản nghiệm thu mẫu (Acceptance Sign-off Template)

Khi thực hiện tích hợp, các bên sẽ đối chiếu và ký xác nhận:

- [ ] **Xác nhận bàn giao Module 01 $\rightarrow$ Module 03 (Bước 16):**
  - Phiên bản reference: `20261004-5b4e04a2`
  - Số lượng phim: 9.742
  - Mã SHA256: *(Khớp với manifest)*
  - Ngày xác nhận: `... / ... / 2026`
  - Người xác nhận: Đại diện Module 01 & Giang (Module 03).

- [ ] **Xác nhận bàn giao Module 03 $\rightarrow$ Module 04:**
  - Trạng thái input: Đã sẵn sàng tại `/movies/ref/current/` và `/raw/ratings/`
  - Trạng thái quyền `/batch`: Write permission OK (777)
  - Ngày xác nhận: `... / ... / 2026`
  - Người xác nhận: Đỗ Sơn (Module 04) & Giang (Module 03).
