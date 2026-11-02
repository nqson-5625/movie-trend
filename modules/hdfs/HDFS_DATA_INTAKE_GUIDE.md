# SỔ TAY TIẾP NHẬN DỮ LIỆU TỪ HDFS (MODULE 03 $\rightarrow$ MODULE 04/05)
## DÀNH CHO BỘ PHẬN XỬ LÝ DỮ LIỆU LỚN (BATCH LAYER & MAPREDUCE)

**Người bàn giao:** Giang (Module 03 - HDFS Storage Layer)  
**Người tiếp nhận:** Đỗ Sơn (Module 04 - Hadoop MapReduce) & Thắng/Hiếu (Module 05)  
**Mục tiêu:** Cung cấp đầy đủ thông tin kỹ thuật, định dạng file, schema, đường dẫn và code mẫu để người tiếp nhận có thể bắt tay vào viết Job xử lý ngay lập tức mà không cần hỏi thêm.

---

## 1. TÓM TẮT 30 GIÂY (QUICK START TL;DR)

| Thông tin | Giá trị bạn cần dùng |
| :--- | :--- |
| **HDFS NameNode RPC** | `hdfs://hdfs-namenode-svc.bigdata.svc.cluster.local:9000` *(trong K8s)*<br>`hdfs://localhost:9000` *(từ máy host/WSL)* |
| **Giao diện Web UI** | [http://localhost:9870](http://localhost:9870) *(xem cây thư mục trực quan trên trình duyệt)* |
| **Dữ liệu danh mục phim (Dim)** | `/movies/ref/current/movie_reference.tsv` *(định dạng TSV, 9.742 phim)* |
| **Dữ liệu đánh giá thô (Fact)** | `/raw/ratings/dt=YYYY-MM-DD/*.jsonl` *(định dạng JSON Lines, đã phân vùng)* |
| **Thư mục ghi kết quả của bạn** | `/batch/` *(đã cấp quyền 777 — toàn quyền tạo thư mục & ghi file)* |
| **Khóa dùng để JOIN** | Trường `movieId` (kiểu số nguyên `int`) |

---

## 2. BẢN ĐỒ DỮ LIỆU HDFS (DATA ARCHITECTURE MAP)

```
HDFS ROOT (/)
 ├── movies/
 │    └── ref/
 │         └── current/
 │              └── movie_reference.tsv      <--- [BẠN ĐỌC] Danh mục phim chuẩn (TSV, 10 cột)
 ├── raw/
 │    └── ratings/
 │         ├── dt=1996-04-12/
 │         │    └── part-*.jsonl             <--- [BẠN ĐỌC] Dữ liệu đánh giá thô (JSON Lines)
 │         ├── dt=1996-04-17/
 │         └── dt=.../
 └── batch/                                  <--- [BẠN GHI] Thư mục kết quả MapReduce (chmod 777)
      ├── movie_stats/run_id=<run_id>/
      └── movie_daily_stats/run_id=<run_id>/
```

---

## 3. CHI TIẾT TẬP DỮ LIỆU 1: CANONICAL MOVIE REFERENCE

* **Đường dẫn HDFS:** `/movies/ref/current/movie_reference.tsv`
* **Mục đích:** Bảng danh mục thông tin phim (Dimension / Lookup table) dùng để ánh xạ tên phim, năm sản xuất, thể loại, điểm IMDb khi tính toán xu hướng.
* **Quy cách kỹ thuật:**
  * File text định dạng **TSV** (Tab-Separated Values, phân tách bằng dấu tab `\t`).
  * Bảng mã: `UTF-8`.
  * Có **dòng tiêu đề (header)** ở dòng đầu tiên.
  * Tổng số dòng: `9,743` dòng (1 header + 9,742 phim).
  * Nếu dữ liệu bị thiếu (ví dụ phim chưa có điểm IMDb), giá trị sẽ để trống hoặc chuỗi rỗng `""`.

### Bảng đặc tả 10 cột dữ liệu:

| Thứ tự | Tên cột | Kiểu dữ liệu | Ví dụ | Ý nghĩa nghiệp vụ |
| :---: | :--- | :--- | :--- | :--- |
| **1** | `movieId` | Integer | `1` | **Khóa chính (Primary Key)**, khớp với `movieId` trong ratings |
| **2** | `title` | String | `Toy Story (1995)` | Tên phim đầy đủ kèm năm |
| **3** | `release_year` | Integer / Null | `1995` | Năm phát hành phim |
| **4** | `genres` | String | `Adventure\|Animation\|Children` | Danh sách thể loại, ngăn cách bởi dấu `\|` |
| **5** | `imdbId` | String | `tt0114709` | Mã định danh chuẩn IMDb |
| **6** | `tmdbId` | Integer / Null | `862` | Mã định danh chuẩn TMDb |
| **7** | `imdb_rating` | Float / Null | `8.3` | Điểm đánh giá trung bình từ IMDb (thang điểm 10) |
| **8** | `imdb_votes` | Integer / Null | `1202835` | Tổng số lượt bình chọn trên IMDb |
| **9** | `ref_version` | String | `20261013-dcf23738` | Phiên bản snapshot của danh mục |
| **10** | `generated_at`| Timestamp ISO | `2026-10-13T07:29:58Z` | Thời điểm tạo danh mục |

#### Dữ liệu mẫu thực tế trích từ HDFS:
```tsv
movieId	title	release_year	genres	imdbId	tmdbId	imdb_rating	imdb_votes	ref_version	generated_at
1	Toy Story (1995)	1995	Adventure|Animation|Children|Comedy|Fantasy	tt0114709	862	8.3	1202835	20261013-dcf23738	2026-10-13T07:29:58Z
2	Jumanji (1995)	1995	Adventure|Children|Fantasy	tt0113497	8844	7.1	420594	20261013-dcf23738	2026-10-13T07:29:58Z
3	Grumpier Old Men (1995)	1995	Comedy|Romance	tt0113228	15602	6.7	32044	20261013-dcf23738	2026-10-13T07:29:58Z
```

---

## 4. CHI TIẾT TẬP DỮ LIỆU 2: RAW RATINGS EVENTS

* **Đường dẫn HDFS:** `/raw/ratings/dt=YYYY-MM-DD/*.jsonl`
* **Mục đích:** Dữ liệu sự kiện người dùng đánh giá phim (Fact / Event stream).
* **Quy cách kỹ thuật:**
  * File text định dạng **JSON Lines** (`.jsonl`): Mỗi dòng là **1 đối tượng JSON độc lập**, phân tách bằng ký tự xuống dòng `\n`.
  * Đã được phân vùng tự động theo ngày: thư mục con dạng `dt=YYYY-MM-DD`.
  * **Lưu ý quan trọng về ngày:** Phân vùng ngày được trích xuất từ thời gian thực tế người dùng đánh giá (`event_time`), không phải thời gian máy chủ nạp dữ liệu.

### Bảng đặc tả các trường JSON:

| Tên trường | Kiểu dữ liệu | Ví dụ | Ý nghĩa nghiệp vụ |
| :--- | :--- | :--- | :--- |
| `schema_version` | String | `"1.0"` | Phiên bản của hợp đồng dữ liệu |
| `event_id` | String | `"0f5bb26d77..."` | Mã SHA-256 duy nhất của sự kiện (dùng để de-duplicate) |
| `source` | String | `"movielens"` | Nguồn gốc dữ liệu (`movielens`) |
| `userId` | Integer | `107` | Mã người dùng thực hiện đánh giá |
| `movieId` | Integer | `1` | **Mã phim được đánh giá (Khóa dùng để JOIN với Reference)** |
| `rating` | Float | `4.0` | Số điểm đánh giá (từ `0.5` đến `5.0`) |
| `event_time` | Timestamp ISO | `"1996-04-12T15:19:00Z"` | Thời điểm đánh giá chuẩn UTC (dùng phân tích xu hướng) |
| `ingest_time` | Timestamp ISO | `"2026-10-13T07:18:47Z"` | Thời điểm hệ thống ingest vào kho HDFS |

#### Dữ liệu mẫu thực tế trích từ HDFS:
```json
{"schema_version": "1.0", "event_id": "0f5bb26d7726f72d954b900b1e3ef96b1d9d76adf4cb8853653ee8d2891f5680", "source": "movielens", "userId": 107, "movieId": 1, "rating": 4.0, "event_time": "1996-04-12T15:19:00Z", "ingest_time": "2026-10-13T07:18:47Z"}
{"schema_version": "1.0", "event_id": "19ce650dfd3908e5fa5bb4806bbb5a3d018d548641d90127adc16618298a8cb2", "source": "movielens", "userId": 107, "movieId": 2, "rating": 5.0, "event_time": "1996-04-12T15:19:00Z", "ingest_time": "2026-10-13T07:18:47Z"}
{"schema_version": "1.0", "event_id": "3e87ba498fb351f485fb90cdd79f7898799f47b12e85bedd5e7d4aa46abfbfaf", "source": "movielens", "userId": 107, "movieId": 5, "rating": 4.0, "event_time": "1996-04-12T15:19:00Z", "ingest_time": "2026-10-13T07:18:47Z"}
```

---

## 5. QUY ƯỚC ĐẦU RA BATCH LAYER (`/batch/`)

Module 03 đã cấp toàn quyền (`drwxrwxrwx` - mode `777`) cho thư mục `/batch/`. Bạn có thể tự do tạo thư mục con và ghi dữ liệu ra mà không bao giờ gặp lỗi phân quyền (`Permission denied`).

### Cấu trúc khuyến nghị khi lưu kết quả:
Khi bạn chạy MapReduce Job, khuyến nghị đặt output path như sau:

```
/batch/
  ├── movie_stats/run_id=20261013-01/part-r-00000.csv       # Tổng hợp thống kê theo phim
  ├── movie_daily_stats/run_id=20261013-01/part-r-00000.csv # Xu hướng điểm theo từng ngày
  └── genre_daily_stats/run_id=20261013-01/part-r-00000.csv # Xu hướng theo thể loại phim
```

---

## 6. CODE MẪU TÍCH HỢP (READY-TO-USE CODE SNIPPETS)

### 6.1. Code mẫu Python (MapReduce Mapper / PySpark / Spark Batch)

Đoạn code dưới đây minh họa cách đọc đồng thời cả 2 nguồn dữ liệu và thực hiện JOIN trên `movieId`:

```python
import csv
import json

# 1. Đọc bảng Reference vào bộ nhớ (Distributed Cache hoặc Dictionary)
movie_catalog = {}
with open("movie_reference.tsv", "r", encoding="utf-8") as f:
    reader = csv.DictReader(f, delimiter="\t")
    for row in reader:
        m_id = int(row["movieId"])
        movie_catalog[m_id] = {
            "title": row["title"],
            "genres": row["genres"].split("|"),
            "imdb_rating": float(row["imdb_rating"]) if row["imdb_rating"] else None,
            "release_year": int(row["release_year"]) if row["release_year"] else None,
        }

print(f"-> Đã nạp thành công {len(movie_catalog)} phim vào bộ nhớ tra cứu.")

# 2. Xử lý stream sự kiện đánh giá (từ HDFS raw ratings jsonl)
def process_rating_record(line: str):
    """Mapper logic: Parse JSON và join với Catalog."""
    record = json.loads(line.strip())
    movie_id = int(record["movieId"])
    rating = float(record["rating"])
    event_time = record["event_time"]
    
    # Thực hiện JOIN
    meta = movie_catalog.get(movie_id, {})
    title = meta.get("title", "Unknown")
    genres = meta.get("genres", ["Unknown"])
    
    # Emit key-value cho Reducer
    return (movie_id, title, rating, genres, event_time)

# Ví dụ test nhanh:
sample_json = '{"userId": 107, "movieId": 1, "rating": 4.0, "event_time": "1996-04-12T15:19:00Z"}'
print("Kết quả Join mẫu:", process_rating_record(sample_json))
```

---

### 6.2. Lệnh chạy Hadoop Streaming MapReduce CLI

Nếu bạn dùng Hadoop Streaming với Python:

```bash
hadoop jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -files hdfs:///movies/ref/current/movie_reference.tsv#movie_reference.tsv,mapper.py,reducer.py \
  -input /raw/ratings/dt=1996-*/*.jsonl \
  -output /batch/movie_stats/run_id=demo_01 \
  -mapper "python3 mapper.py" \
  -reducer "python3 reducer.py"
```

---

## 7. CÁC LỆNH TIỆN ÍCH KIỂM TRA HDFS (CLI CHEAT SHEET)

Bạn có thể chạy các lệnh này từ terminal WSL để kiểm tra dữ liệu bất kỳ lúc nào:

```bash
# 1. Xem danh sách các thư mục phân vùng ngày:
kubectl exec -i -n bigdata hdfs-namenode-0 -c namenode -- hdfs dfs -ls /raw/ratings

# 2. Đọc thử 5 dòng đánh giá thô bất kỳ:
kubectl exec -i -n bigdata hdfs-namenode-0 -c namenode -- hdfs dfs -cat /raw/ratings/dt=1996-04-12/*.jsonl | head -n 5

# 3. Đọc 10 dòng đầu của file Reference:
kubectl exec -i -n bigdata hdfs-namenode-0 -c namenode -- hdfs dfs -cat /movies/ref/current/movie_reference.tsv | head -n 10

# 4. Kiểm tra quyền ghi vào thư mục batch:
kubectl exec -i -n bigdata hdfs-namenode-0 -c namenode -- hdfs dfs -ls -d /batch

# 5. Tự động kiểm tra tính sẵn sàng của môi trường bàn giao:
cd /home/giang/movie-trend && make -C modules/hdfs verify-mr
```

---

## 8. LIÊN HỆ HỖ TRỢ KỸ THUẬT

Nếu trong quá trình triển khai Job MapReduce gặp bất kỳ vấn đề nào về kết nối mạng HDFS, phân quyền hoặc format dữ liệu:
* **Đầu mối HDFS:** Giang (Module 03)
* **Tài liệu chi tiết hệ thống:** `modules/hdfs/README.md` và `BAO_CAO_KET_QUA_VAN_HANH_REAL_DATA.md`
