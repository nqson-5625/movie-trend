# M02-13 — Báo cáo hiệu năng Kafka ingestion

Đo ngày 11/10/2026 trên WSL2; thời điểm UTC chính xác trong [performance.json](performance.json).
Input: MovieLens thật đã được Module 01 sort; SHA-256 trong [acceptance.json](acceptance.json).

## Phương pháp

WSL được cấp 3917 MiB RAM (khoảng 4 GB), swap 4 GB; máy mục tiêu có 8 GB RAM vật lý. Không đo trực tiếp Windows host.
Một broker KRaft riêng, 1 partition/topic, RF=1, heap 256–512 MiB; Python producer và consumer chạy đồng thời trong một process, batch 100, acks=all, idempotence, lz4. Broker local và các ứng dụng khác vẫn có thể chạy nền.
Mỗi rate dùng các dòng nguồn kế tiếp; consumer commit đồng bộ từng record và ghi SQLite FULL trước commit. RSS lấy mẫu mỗi 200 ms, CPU là phần trăm của một core trong thời gian chạy. Peak RSS có thể bỏ lỡ spike ngắn. Throughput consumer bao gồm thời gian group assignment và hoàn tất, không phải chỉ vòng poll.
Timestamp ingest có độ chính xác một giây. Latency là consume_time − ingest_time (có sai số lượng tử dưới một giây), không dùng event_time lịch sử.

## Kết quả

| Rate cấu hình | Số event | ACK / fail | Producer events/s | Consumer events/s | Lag cuối | Latency mean / max (s) | RSS Python / Kafka (MiB) | CPU Python / Kafka (%) |
| ---: | ---: | --- | ---: | ---: | ---: | --- | --- | --- |
| 10 | 200 | 200 / 0 | 9.98 | 9.54 | 0 | 0.892 / 4.752 | 41.7 / 386.7 | 3.1 / 22.1 |
| 100 | 1000 | 1000 / 0 | 98.81 | 89.90 | 0 | 0.671 / 2.110 | 43.4 / 408.5 | 13.9 / 39.8 |
| 500 | 2000 | 2000 / 0 | 451.58 | 368.09 | 0 | 0.888 / 1.468 | 45.5 / 423.1 | 35.3 / 62.1 |

Tổng 3.200 ACK; consumer xác minh đủ 3.200 identity mới, 0 invalid/duplicate/conflict/offset regression/gap; lag cuối mỗi run bằng 0. RAM khả dụng thấp nhất trong WSL khoảng 933 MiB.
Mức 10 có ảnh hưởng startup/coordinator warmup (latency max ~4,75 s). Mức 500 thực đạt ~452 events/s: rate limiter là trần tốc độ, không bảo đảm đạt target sau chi phí validation, flush/checkpoint và I/O.

## Cấu hình cho máy 8 GB

- Mặc định `RATE=100 BATCH_SIZE=100`, Kafka heap 512 MiB, một broker/partition. Giữ giới hạn WSL khoảng 4 GB như hướng dẫn setup để còn RAM cho Windows.
- Dùng 10 events/s để demo; có thể thử 500 khi Kafka chạy riêng và lag vẫn về 0. Không mặc định 500 khi cùng chạy Spark/HDFS/Elasticsearch.
- Queue producer/consumer giới hạn 8 MiB; CSV đọc streaming, dedup/audit nằm SQLite trên disk. Theo dõi dung lượng audit và Kafka retention khi chạy lâu.
- Đây là phép đo ngắn (200/1000/2000 event), chưa phải soak test toàn dataset hay chứng nhận đủ RAM cho toàn bộ Lambda stack. Cần đo lại trên máy triển khai trước khi tăng rate.

## Chạy lại

```bash
make -C modules/kafka_ingestion benchmark
```

Lệnh tạo broker thử nghiệm riêng, ghi đè `reports/performance.json` bằng phép đo mới và dừng broker sau khi xong. Báo cáo Markdown này lưu kết quả của lần đo nêu trên; cập nhật bảng khi thay artifact JSON.

Smoke API thật chạy riêng sau producer: 100 ratings + 20 TMDB records, 120/120 valid, lag 0. Latency smoke bao gồm thời gian đợi gọi API trước khi consumer khởi động nên không dùng để đánh giá throughput đồng thời.
