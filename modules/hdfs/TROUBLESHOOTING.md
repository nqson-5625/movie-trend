# Sổ tay Xử lý Sự cố HDFS (Troubleshooting Guide) — Module 03

Tài liệu này tổng hợp các lỗi phổ biến nhất khi vận hành cụm Hadoop HDFS trong đồ án **Movie-Trend** và cách khắc phục nhanh chóng.

---

## 1. Bảng tra cứu sự cố nhanh

| Triệu chứng | Nguyên nhân cốt lõi | Cách xử lý tức thì |
| :--- | :--- | :--- |
| **"Name node is in safe mode"** | NameNode vừa khởi động hoặc thiếu tỷ lệ block report từ DataNodes. | `docker exec -it hdfs-namenode hdfs dfsadmin -safemode leave` |
| **DataNode crash, log "Incompatible clusterIDs"** | Format NameNode nhiều lần làm lệch ClusterID với DataNode volume. | Xóa volume cũ: `docker compose down -v` rồi khởi động lại. |
| **Container bị tắt bất ngờ (Exit 137)** | Linux OOM Killer tiêu diệt tiến trình Java do cạn RAM máy ảo. | Kiểm tra `dmesg -T \| grep oom`, cấu hình Heap `-Xmx768m`, tạo swap 4GB. |
| **WebHDFS báo Connection Refused khi upload** | Lỗi chuyển hướng HTTP 307 không phân giải được hostname `datanode1`. | Thêm `127.0.0.1 namenode datanode1 datanode2` vào file `hosts`. |
| **Permission Denied khi ghi file vào HDFS** | Cấu hình kiểm tra quyền HDFS đang bật khi dev nội bộ. | Đặt `HDFS-SITE.XML_dfs.permissions.enabled=false` trong `docker-compose.yml`. |

---

## 2. Hướng dẫn chi tiết từng trường hợp

### 2.1. NameNode kẹt ở chế độ Safe Mode
- **Hiện tượng:** Mọi lệnh ghi hoặc tạo thư mục đều báo lỗi: `Cannot create directory... Name node is in safe mode`.
- **Cách kiểm tra:**
  ```bash
  docker exec -it hdfs-namenode hdfs dfsadmin -safemode get
  ```
- **Cách khắc phục:**
  ```bash
  docker exec -it hdfs-namenode hdfs dfsadmin -safemode leave
  ```

---

### 2.2. DataNode không kết nối được (ClusterID Mismatch)
- **Hiện tượng:** Lệnh `hdfs dfsadmin -report` chỉ hiển thị `Live datanodes (0)` hoặc `Live datanodes (1)`.
- **Xem log DataNode:**
  ```bash
  docker logs --tail 50 hdfs-datanode1
  ```
  Nếu thấy lỗi `java.io.IOException: Incompatible clusterIDs`:
- **Cách khắc phục:**
  ```bash
  cd modules/hdfs
  make down
  # Xóa các volumes cũ để format đồng bộ
  docker volume prune -f
  make up
  make init
  ```

---

### 2.3. Tránh tràn RAM trên máy ảo 8 GB (OOM Killer)
- **Tạo Swap file 4 GB trên Ubuntu:**
  Nếu máy ảo Ubuntu chỉ có 8 GB RAM và chạy đồng thời Kafka + Spark + HDFS + Elasticsearch:
  ```bash
  sudo fallocate -l 4G /swapfile
  sudo chmod 600 /swapfile
  sudo mkswap /swapfile
  sudo swapon /swapfile
  echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
  ```
- **Kiểm tra RAM tiêu thụ thực tế:**
  ```bash
  docker stats --no-stream hdfs-namenode hdfs-datanode1 hdfs-datanode2
  ```
  Đảm bảo tổng RAM của cả 3 containers $\le 1.5\text{ GB}$.

---

### 2.4. Khắc phục lỗi chuyển hướng WebHDFS (HTTP 307)
- WebHDFS upload dữ liệu theo 2 giai đoạn:
  1. Gửi `PUT` tới NameNode `http://localhost:9870/webhdfs/v1/...`
  2. NameNode phản hồi mã HTTP `307 Temporary Redirect` tới DataNode `http://datanode1:9864/...`
- Nếu máy host không hiểu `datanode1`, hãy sửa file `/etc/hosts` (trên Linux) hoặc `C:\Windows\System32\drivers\etc\hosts` (trên Windows):
  ```text
  127.0.0.1 namenode datanode1 datanode2
  ```

---

### 2.5. Các lệnh kiểm tra sức khỏe và dọn dẹp định kỳ
- **Kiểm tra toàn bộ hệ thống file HDFS:**
  ```bash
  docker exec -it hdfs-namenode hdfs fsck / -files -blocks
  ```
- **Xem dung lượng từng thư mục:**
  ```bash
  docker exec -it hdfs-namenode hdfs dfs -du -h /
  ```
