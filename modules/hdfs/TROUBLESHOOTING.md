# Sổ tay Xử lý Sự cố HDFS (Troubleshooting Guide) — Module 03

Tài liệu này tổng hợp các lỗi phổ biến nhất khi vận hành cụm Hadoop HDFS trong đồ án **Movie-Trend** trên môi trường **Kubernetes (k3s)**, kèm hướng dẫn xử lý nhanh chóng cho người chạy cuối cùng.

---

## 1. Bảng tra cứu sự cố nhanh

| Triệu chứng | Môi trường | Nguyên nhân cốt lõi | Cách xử lý tức thì |
| :--- | :---: | :--- | :--- |
| **"Name node is in safe mode"** | Kubernetes | NameNode vừa khởi động hoặc thiếu tỷ lệ block report từ DataNodes. | `kubectl exec -it -n bigdata pod/hdfs-namenode-0 -- hdfs dfsadmin -safemode leave` |
| **Pod báo `CrashLoopBackOff`** | Kubernetes | NameNode chưa được format hoặc lỗi quyền ghi thư mục PVC. | Xem log InitContainer: `kubectl logs pod/hdfs-namenode-0 -c init-format-namenode -n bigdata`. |
| **DataNode crash, log "Incompatible clusterIDs"** | Kubernetes | Format NameNode nhiều lần làm lệch ClusterID với DataNode volume. | `make -C modules/hdfs clean && make -C modules/hdfs up` |
| **Pod bị tắt (Exit 137, OOMKilled)** | Kubernetes | Linux OOM Killer tiêu diệt JVM do cạn RAM máy ảo 8 GB. | Kiểm tra `kubectl describe pod hdfs-namenode-0 -n bigdata`, đảm bảo JVM Heap `-Xmx768m`, kích hoạt 4GB swap. |
| **PVC kẹt ở trạng thái `Pending`** | Kubernetes | Chưa có StorageClass mặc định hoặc node hết dung lượng ổ đĩa. | Kiểm tra `kubectl get sc`. Trên k3s StorageClass mặc định là `local-path`. Kiểm tra dung lượng: `df -h`. |
| **WebHDFS báo Connection Refused khi upload** | Kubernetes | Lỗi HTTP 307 redirect không phân giải được hostname DataNode từ host. | Xem mục 2.4: dùng `make publish-ref` (sử dụng `kubectl cp` trực tiếp). |
| **Permission Denied khi ghi file vào HDFS** | Kubernetes | Cấu hình kiểm tra quyền HDFS đang bật (`dfs.permissions.enabled=true`). | Kiểm tra ConfigMap [01-configmap.yaml](file:///d:/2026.1/LTVXLDLL/BTL/modules/hdfs/k8s/01-configmap.yaml): đặt `dfs.permissions.enabled=false`. |

---

## 2. Hướng dẫn chi tiết từng trường hợp

### 2.1. NameNode kẹt ở chế độ Safe Mode
- **Hiện tượng:** Mọi lệnh ghi hoặc tạo thư mục đều báo lỗi: `Cannot create directory... Name node is in safe mode`.
- **Cách kiểm tra:**
  ```bash
  kubectl exec -it -n bigdata pod/hdfs-namenode-0 -- hdfs dfsadmin -safemode get
  ```
- **Cách khắc phục:**
  ```bash
  kubectl exec -it -n bigdata pod/hdfs-namenode-0 -- hdfs dfsadmin -safemode leave
  ```

---

### 2.2. Pod NameNode bị `CrashLoopBackOff` trên Kubernetes
- **Hiện tượng:** `kubectl get pods -n bigdata` hiển thị status `CrashLoopBackOff` hoặc `Error`.
- **Kiểm tra nguyên nhân:**
  1. Kiểm tra log của InitContainer format:
     ```bash
     kubectl logs pod/hdfs-namenode-0 -c init-format-namenode -n bigdata
     ```
  2. Kiểm tra log của container chính:
     ```bash
     kubectl logs pod/hdfs-namenode-0 -n bigdata --tail 50
     ```
- **Khắc phục:**
  - Nếu báo thiếu thư mục hoặc lỗi format: Cơ chế `init-format-namenode` trong manifest [02-namenode.yaml](file:///d:/2026.1/LTVXLDLL/BTL/modules/hdfs/k8s/02-namenode.yaml) sẽ tự động kiểm tra thư mục `current/VERSION`. Nếu PVC cũ bị lỗi hỏng cấu trúc, hãy dọn dẹp bằng:
    ```bash
    make -C modules/hdfs clean
    make -C modules/hdfs up
    ```

---

### 2.3. DataNode không kết nối được (ClusterID Mismatch)
- **Hiện tượng:** Lệnh `hdfs dfsadmin -report` chỉ hiển thị `Live datanodes (0)` hoặc `Live datanodes (1)`.
- **Xem log DataNode:**
  ```bash
  kubectl logs pod/hdfs-datanode-0 -n bigdata --tail 50
  ```
  Nếu thấy lỗi `java.io.IOException: Incompatible clusterIDs`:
- **Cách khắc phục triệt để:**
  Xóa sạch cụm và PVC để khởi tạo lại đồng bộ từ đầu:
  ```bash
  make -C modules/hdfs clean
  make -C modules/hdfs up
  make -C modules/hdfs init
  ```

---

### 2.4. Tránh tràn RAM trên máy ảo 8 GB (OOM Killer)
- **Tạo Swap file 4 GB trên Ubuntu:**
  Nếu máy ảo Ubuntu chỉ có 8 GB RAM và chạy đồng thời Kafka + Spark + HDFS + Elasticsearch:
  ```bash
  sudo fallocate -l 4G /swapfile
  sudo chmod 600 /swapfile
  sudo mkswap /swapfile
  sudo swapon /swapfile
  echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
  ```
- **Kiểm tra RAM tiêu thụ thực tế của cụm HDFS:**
  ```bash
  kubectl top pods -n bigdata
  ```
  Đảm bảo tổng RAM của cả cụm HDFS luôn $\le 1.5\text{ GB}$.

---

### 2.5. Khắc phục lỗi chuyển hướng WebHDFS (HTTP 307 Redirect)
- WebHDFS upload dữ liệu theo 2 giai đoạn:
  1. Client gửi `PUT` tới NameNode `http://localhost:9870/webhdfs/v1/...`
  2. NameNode phản hồi mã HTTP `307 Temporary Redirect` tới DataNode `http://<datanode-host>:9864/...`
- **Khắc phục trên Kubernetes:**
  - Trong file [01-configmap.yaml](file:///d:/2026.1/LTVXLDLL/BTL/modules/hdfs/k8s/01-configmap.yaml) đã cấu hình:
    ```xml
    <property>
      <name>dfs.datanode.use.datanode.hostname</name>
      <value>true</value>
    </property>
    ```
  - Khi chạy upload reference từ ngoài cụm, script [publish_reference.py](file:///d:/2026.1/LTVXLDLL/BTL/modules/hdfs/scripts/publish_reference.py) (hoặc lệnh `make publish-ref`) sử dụng `kubectl cp` trực tiếp vào Pod NameNode rồi chạy `hdfs dfs -put`, **hoàn toàn loại trừ lỗi chuyển hướng HTTP 307**!

---

### 2.6. Các lệnh kiểm tra sức khỏe và dọn dẹp định kỳ

- **Kiểm tra toàn bộ hệ thống file HDFS (quét blocks hỏng):**
  ```bash
  kubectl exec -it -n bigdata pod/hdfs-namenode-0 -- hdfs fsck / -files -blocks
  ```

- **Xem dung lượng từng phân vùng HDFS:**
  ```bash
  kubectl exec -it -n bigdata pod/hdfs-namenode-0 -- hdfs dfs -du -h /
  ```

- **Kiểm tra chi tiết trạng thái cụm:**
  ```bash
  make -C modules/hdfs report
  ```

