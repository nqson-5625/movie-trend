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
- **Vấn đề đã giải quyết:** Trong [modules/data_source/README.md](../data_source/README.md) nêu rõ: *"Bước 16: Chờ HDFS endpoint/path để publish reference"*. Module 03 đã cung cấp trọn vẹn giải pháp này trên Kubernetes.
- **Endpoint cung cấp:**
  * **RPC NameNode:** `hdfs://hdfs-namenode-svc.bigdata.svc.cluster.local:9000`
  * **WebHDFS REST API:** `http://hdfs-namenode-svc.bigdata.svc.cluster.local:9870/webhdfs/v1`
  * **Host Port-Forward:** `http://localhost:9870`
- **Quy trình nghiệm thu 4 bước:**
  1. Module 03 khởi động cụm HDFS Kubernetes (`make up`) và kiểm tra NameNode sẵn sàng.
  2. File `data/prepared/reference/movie_reference.tsv` được tải lên `/movies/ref/version=<ref_version>/movie_reference.tsv`.
  3. Kiểm tra tính toàn vẹn:
     - Số dòng phải là: **9.743 dòng** (9.742 phim + 1 header).
     - Checksum SHA256 trên HDFS khớp 100% với file local manifest.
  4. Cập nhật atomic sang canonical path: `/movies/ref/current/movie_reference.tsv`.
- **Lệnh thực hiện:**
  ```bash
  make -C modules/hdfs publish-ref
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
  make -C modules/hdfs sink
  # Kiểm thử tự động luồng tích hợp Kafka-HDFS:
  make -C modules/hdfs test-sink
  ```

---

### 2.3. Handoff với Module 04 (Hadoop MapReduce - Đỗ Sơn)
- **Dữ liệu đầu vào (Input):**
  - Ratings: `hdfs://hdfs-namenode-svc.bigdata.svc.cluster.local:9000/raw/ratings/dt=*/part-*.jsonl`
  - Canonical Reference: `hdfs://hdfs-namenode-svc.bigdata.svc.cluster.local:9000/movies/ref/current/movie_reference.tsv`
- **Dữ liệu đầu ra (Output):**
  - Thư mục `/batch/` đã được mở quyền `chmod 777`.
  - Cấu trúc thư mục con cho từng batch job:
    * `/batch/movie_stats/run_id=<run_id>/`
    * `/batch/movie_daily_stats/run_id=<run_id>/`
    * `/batch/genre_daily_stats/run_id=<run_id>/`
    * `/batch/_meta/run_id=<run_id>/manifest.json`
- **Tối ưu tài nguyên Pod DataNode (Khắc phục OOMKilled):**
  - Trong `modules/hdfs/k8s/03-datanode.yaml`, cấu hình JVM Heap của DataNode đã được điều chỉnh thành:
    `-Xms128m -Xmx256m -XX:+UseG1GC` (so với mức cũ `-Xmx384m` quá sát giới hạn `limits.memory: 512Mi` của Pod).
  - Cấu hình này dành ra khoảng 256 MiB đệm cho Native memory, Metaspace, thread stacks và I/O buffers, giúp DataNode hoạt động hoàn toàn ổn định khi Module 04 chạy các MapReduce jobs đọc/ghi dữ liệu song song mà không bị Kubernetes OOMKilled (Exit Code 137).
- **Lưu ý chất lượng dữ liệu & Deduplication:**
  - Trong `/raw/ratings` có ghi nhận **1.000 dòng bị trùng `event_id`** (nguyên nhân: Ingestion Sink bị trễ I/O trong một đợt ghi dẫn đến vượt ngưỡng `max.poll.interval.ms` của Kafka Consumer, Kafka kích hoạt rebalance và sink đọc lại một lô 1.000 events).
  - **Module 04 đã xử lý:** Đỗ Sơn (Module 04) đã tích hợp cơ chế loại trùng (deduplication theo `event_id`) trong tầng MapReduce, do đó kết quả tính toán batch (`movie_stats`, `genre_daily_stats`, `movie_daily_stats`) hoàn toàn chính xác.
- **QUY TẮC VẬN HÀNH BẮT BUỘC (CRITICAL NOTE):**
  > [!IMPORTANT]
  > **TUYỆT ĐỐI KHÔNG CHẠY LẠI PRODUCER KHI `/raw` ĐÃ CÓ DỮ LIỆU!**  
  > Do cơ chế HDFS Sink daemon là streaming append vào các file theo phân vùng ngày `dt=YYYY-MM-DD`, việc kích hoạt lại Kafka Producer (`make producer` hoặc `movie_producer.py`) khi dữ liệu thô đã tồn tại trên HDFS sẽ làm tăng sinh thêm các sự kiện trùng lặp.  
  > Nếu muốn chạy lại pipeline từ đầu để nghiệm thu hoặc đo benchmark: bắt buộc phải xóa sạch dữ liệu cũ trong `/raw/ratings` trên HDFS trước (hoặc chạy lại chu trình khởi tạo `make clean && make init`).
- **Lệnh kiểm tra:**
  ```bash
  make -C modules/hdfs verify-mr
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
  - Cấu hình DataNode: Đã cập nhật `-Xms128m -Xmx256m` trong `03-datanode.yaml` (MapReduce chạy mượt, không bị OOMKilled)
  - Dữ liệu thô `/raw/ratings`: Ghi nhận 1.000 dòng trùng do `max.poll.interval.ms` — Module 04 đã loại trùng thành công
  - Quy tắc vận hành: Đã thống nhất KHÔNG chạy lại producer khi `/raw` đã có dữ liệu
  - Ngày xác nhận: `... / ... / 2026`
  - Người xác nhận: Đỗ Sơn (Module 04) & Giang (Module 03).
