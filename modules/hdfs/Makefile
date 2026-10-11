# Makefile for Module 03: HDFS Storage Layer
# Phụ trách: Giang (Module 03)

COMPOSE_FILE = docker/docker-compose.yml
CONTAINER_NAMENODE = hdfs-namenode
PYTHON ?= python3

.PHONY: help up down restart ps report init test publish-ref verify-mr run-sink stats clean

help:
	@echo "Các lệnh điều khiển Module 03 HDFS:"
	@echo "  make up          - Khởi động cụm HDFS (1 NameNode + 2 DataNodes)"
	@echo "  make down        - Dừng cụm HDFS"
	@echo "  make restart     - Khởi động lại cụm HDFS"
	@echo "  make ps          - Xem trạng thái containers HDFS"
	@echo "  make report      - Kiểm tra báo cáo dfsadmin -report"
	@echo "  make init        - Khởi tạo cây thư mục chuẩn trên HDFS"
	@echo "  make test        - Chạy kiểm thử tự động luồng Module 01 -> Module 03"
	@echo "  make publish-ref - Nghiệm thu bàn giao Bước 16 (Publish canonical reference)"
	@echo "  make verify-mr   - Kiểm tra môi trường cho Module 04 (MapReduce)"
	@echo "  make run-sink    - Chạy Ingestion Daemon kéo dữ liệu Kafka -> HDFS"
	@echo "  make stats       - Đo RAM tiêu thụ của cụm HDFS"
	@echo "  make clean       - Dừng cụm và xóa toàn bộ dữ liệu volumes"

up:
	docker compose -f $(COMPOSE_FILE) up -d
	@echo "Chờ cụm HDFS sẵn sàng..."
	@sleep 5
	docker exec -it $(CONTAINER_NAMENODE) hdfs dfsadmin -safemode get

down:
	docker compose -f $(COMPOSE_FILE) down

restart:
	docker compose -f $(COMPOSE_FILE) restart

ps:
	docker compose -f $(COMPOSE_FILE) ps

report:
	docker exec -it $(CONTAINER_NAMENODE) hdfs dfsadmin -report

init:
	bash scripts/init_hdfs_dirs.sh $(CONTAINER_NAMENODE)

test:
	$(PYTHON) tests/test_flow_module_1_to_3.py

publish-ref:
	$(PYTHON) scripts/publish_reference.py

verify-mr:
	bash scripts/verify_mapreduce_handoff.sh $(CONTAINER_NAMENODE)

run-sink:
	$(PYTHON) scripts/kafka_hdfs_sink.py

stats:
	docker stats --no-stream hdfs-namenode hdfs-datanode1 hdfs-datanode2

clean:
	docker compose -f $(COMPOSE_FILE) down -v
	@echo "Đã dọn dẹp sạch sẽ volumes HDFS."
