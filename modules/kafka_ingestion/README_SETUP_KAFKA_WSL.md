# M02-01 → M02-06 — Cài Kafka tự động trên WSL2 Ubuntu

> Dành cho development **local**, không phải deployment production/Kubernetes. Bộ setup dùng **Apache Kafka 4.3.1, Java 17, KRaft combined broker/controller, một node**. Không sử dụng ZooKeeper, Docker hay Spark/Hadoop.

## 1. Yêu cầu ban đầu

- Windows 11/Windows 10 hỗ trợ WSL2; đã cài **Ubuntu 24.04 trên WSL2** và có user thường với quyền `sudo`.
- Mở project trong **VS Code → WSL: Ubuntu**; repository nằm trong Linux filesystem, ví dụ `~/movie-trend` (không phải `/mnt/c/...`).
- Có Internet để tải các package Ubuntu và file Kafka từ Apache.
- Kiểm tra **ổ C của Windows** còn dung lượng thực, không dựa vào số `1007G` trong `df -h /` (đó có thể là giới hạn VHDX). Dành dư vài GB cho binary, package, log và topic data.
- Với máy có **RAM 8GB vật lý**, có thể đặt WSL `.wslconfig` như dự án: `memory=4GB`, `processors=4`, `swap=4GB`. Nếu dùng cấu hình khác, hãy thống nhất với nhóm. Script **không** tự chỉnh `.wslconfig` hay restart WSL.

Script **không yêu cầu** cài trước Git/Python venv cho Kafka; nó sẽ cài Java 17 và các tiện ích hệ thống còn thiếu. Nó có sử dụng Python 3 hệ thống (không cần pip) để phát hiện tiến trình Kafka an toàn.

## 2. Chạy một lệnh thiết lập toàn bộ Kafka

Trong **Terminal WSL Ubuntu** của VS Code, tại thư mục repository:

```bash
cd ~/movie-trend
bash modules/kafka_ingestion/setup_kafka_wsl.sh
```

Có thể được yêu cầu nhập mật khẩu `sudo` của **Ubuntu**. Chờ quá trình tải binary Kafka (khoảng 130 MB), kiểm tra SHA-512 và khởi động broker. Không chạy script này bằng `sudo bash`.

Kết quả thành công kết thúc bằng:

```text
[PASS] SHA512_PASS                    # chỉ hiển thị ở lần đầu tải/cài
[PASS] KRaft storage verified (...)
[PASS] BROKER_READY (Kafka CLI metadata request succeeded)
[PASS] KRAFT_QUORUM_READY (LeaderId=1, ClusterId matches storage)
[PASS] SETUP_KAFKA_PASS
```

Nếu có `[ERROR]`, **dừng và gửi lỗi cho người phụ trách**; không xóa thư mục dữ liệu, không tự format lại storage.

## 3. Xác minh sau cài (chỉ đọc)

```bash
bash modules/kafka_ingestion/setup_kafka_wsl.sh --check
```

Mong đợi:

```text
[PASS] Kafka binaries verified
[PASS] KRaft storage verified (...)
[PASS] BROKER_READY
[PASS] KRAFT_QUORUM_READY
[PASS] SETUP_KAFKA_PASS (read-only check)
```

Lệnh `--check` **không tải/cài/gỡ package, không format, không khởi động Kafka**. Nó yêu cầu Kafka đang chạy để xác minh broker và quorum.

## 4. Những gì script thực hiện

| Giai đoạn | Thao tác |
|---|---|
| M02-01 | Kiểm tra/cài **OpenJDK 17 headless** từ `apt`; đặt `JAVA_HOME` cho phiên chạy script, không sửa global PATH/JAVA_HOME |
| M02-02 | Tải `kafka_2.13-4.3.1.tgz` từ `downloads.apache.org`, xác minh SHA-512 với file chính thức **và digest pin cố định** |
| M02-03 | Giải nén `/opt/kafka_2.13-4.3.1`, tạo symlink `/opt/kafka`, kiểm tra Kafka CLI |
| M02-04 | Tạo config `~/kafka-local/config/server.properties` (loopback 9092/9093, KRaft dynamic quorum, 1 broker, RF=1, không auto-create topics) |
| M02-05 | Tạo `cluster.id` và format storage `~/kafka-local/data` với `--standalone` **CHỈ KHI THƯ MỤC RỖNG** |
| M02-06 | Khởi động Kafka bằng CLI với **JVM heap 512MiB**, kiểm tra kết nối broker, ClusterId và LeaderId=1 |

Cấu trúc trên từng máy:

```text
/opt/kafka -> /opt/kafka_2.13-4.3.1  (binary chung, do root sở hữu)
~/kafka-local/
  config/server.properties             (local, không commit)
  data/                                (KRaft metadata + Kafka topic logs)
  logs/                                (Kafka stdout/server logs)
  cluster.id                           (ID local, không commit)
~/downloads/kafka/                      (file .tgz + .sha512 cache)
~/movie-trend/modules/kafka_ingestion/  (source code được Git theo dõi)
```

`PLAINTEXT://127.0.0.1:9092` chỉ dành cho ứng dụng chạy **trong cùng WSL distro**. Không mở listener ra Internet. Khi đưa lên k3s, địa chỉ, replication, bảo mật và tài nguyên phải được thiết kế lại.

## 5. Sau M02-06: chạy Python automation M02-07+

`setup_kafka_wsl.sh` **không tạo topic**, không cài `confluent-kafka` và không chạy producer/consumer. Sau khi project Python venv đã có và dependencies Module 02 đã được cài:

```bash
cd ~/movie-trend
make -C modules/kafka_ingestion setup
make -C modules/kafka_ingestion test
make -C modules/kafka_ingestion topics
make -C modules/kafka_ingestion topics-check
```

Các topic chính thức: `movie-events` (MovieLens **rating interactions**, không phải watch/view logs) và `tmdb-snapshots`.

## 6. Lần sau mở lại Ubuntu

Kafka **không tự khởi động** khi WSL dừng. Dùng Python lifecycle trong module:

```bash
make -C modules/kafka_ingestion kafka-start
make -C modules/kafka_ingestion kafka-status
# Làm việc với Kafka...
make -C modules/kafka_ingestion kafka-stop
```

Hoặc chạy lại script setup: thao tác cài/format sẽ được bỏ qua nếu trạng thái đã có và đúng. Tuy nhiên để dùng thường ngày, ưu tiên các lệnh Makefile ở trên.

## 7. An toàn khi chạy lại / Xử lý lỗi

- **Không bao giờ chạy lại `kafka-storage.sh format` theo cách thủ công** khi data đã tồn tại. Script sẽ phát hiện `meta.properties` và giữ nguyên Cluster ID. Nếu data không rỗng mà thiếu metadata, script **từ chối format**.
- Nếu `/opt/kafka` có sẵn nhưng trỏ sang phiên bản khác, hoặc config hiện có khác thiết kế, script **dừng**, không ghi đè. Liên hệ nhóm để xác định bản đúng trước khi đổi.
- Nếu cổng 9092 hoặc 9093 có tiến trình khác chiếm, script **dừng**; không tự kill service khác.
- Nếu `SHA512_MISMATCH` / `APACHE_SHA512_CHANGED`, **không giải nén**. Xem lại file nguồn/digest trước khi tải lại có kiểm soát.
- Nếu Kafka khởi động thất bại, xem `~/kafka-local/logs/kafkaServer.out` và `server.log` (nếu có). **Không reset storage** để xử lý lỗi kết nối.
- Đừng đưa các thư mục `~/kafka-local`, `~/downloads/kafka`, `.venv`, `.env`, key/token, Kafka topic logs vào Git. ZIP này chỉ chứa script và tài liệu, không chứa binary hoặc dữ liệu.
- SHA-512 hỗ trợ kiểm tra tính toàn vẹn và bản phát hành đúng. Khi cần chính sách xác minh mạnh hơn, bổ sung kiểm tra chữ ký PGP từ Apache.

## 8. Tham khảo chính thức

- [Apache Kafka Quickstart](https://kafka.apache.org/quickstart/) — Java 17+, `random-uuid`, `format --standalone`, `kafka-server-start.sh`.
- [Apache Kafka 4.3.1 releases](https://downloads.apache.org/kafka/4.3.1/) — binary và SHA-512 chính thức.
- [Kafka KRaft](https://kafka.apache.org/43/operations/kraft/) — dynamic quorum, listeners và chế độ controller.

## M02-08 → M02-14

Producer MovieLens/TMDB, consumer, replay/checkpoint, integration tests và benchmark
được hướng dẫn đầy đủ tại [README Module 02](README.md). Sau `topics-check`,
chạy `make -C modules/kafka_ingestion producer LIMIT=100`, `tmdb`, và `consumer`.
