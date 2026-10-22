# Run from movie-trend root with: make -C modules/spark_streaming <target>
# Or directly from modules/spark_streaming with: make <target>
.DEFAULT_GOAL := help
.NOTPARALLEL:

MODULE_MAKEFILE := $(abspath $(lastword $(MAKEFILE_LIST)))
MODULE_DIR := $(abspath $(dir $(MODULE_MAKEFILE)))
PROJECT_ROOT := $(abspath $(MODULE_DIR)/../..)
WORKSPACE_ROOT := $(abspath $(PROJECT_ROOT)/..)

# Tự động nhận diện Python trong .venv (ở movie-trend/.venv hoặc bigdata_prj/.venv hoặc PATH)
ifneq ($(wildcard $(PROJECT_ROOT)/.venv/bin/python),)
    PYTHON ?= $(PROJECT_ROOT)/.venv/bin/python
else ifneq ($(wildcard $(WORKSPACE_ROOT)/.venv/bin/python),)
    PYTHON ?= $(WORKSPACE_ROOT)/.venv/bin/python
else
    PYTHON ?= python3
endif

KAFKA_PACKAGE ?= org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1
KAFKA_BOOTSTRAP ?= 127.0.0.1:9092
ES_URL ?= http://localhost:9200
NAMESPACE ?= bigdata
K8S_MANIFEST := $(MODULE_DIR)/k8s/01-spark-streaming.yaml

.PHONY: help setup test sample run-sample kafka run-kafka up down status logs clean

help:
	@printf '%s\n' \
	  '================================================================================' \
	  'Module 05 - Spark Structured Streaming (Speed Layer)' \
	  'Usage: make -C modules/spark_streaming <target> hoặc cd modules/spark_streaming && make <target>' \
	  '================================================================================' \
	  '' \
	  'Lệnh phát triển & kiểm thử (Local / WSL):' \
	  '  setup        Cài đặt thư viện trong requirements.txt vào virtualenv' \
	  '  test         Chạy toàn bộ Unit Test & Acceptance Test 21.4 với pytest' \
	  '  run-sample   Chạy chế độ sample (đọc & xuất dữ liệu tại data/test_data/)' \
	  '  run-kafka    Chạy Spark Structured Streaming lắng nghe Kafka thật' \
	  '  clean        Xóa các file output và checkpoint tạm (giữ nguyên file input mẫu)' \
	  '' \
	  'Lệnh điều khiển Kubernetes (k3s namespace: bigdata):' \
	  '  up           Triển khai Deployment spark-speed-streaming lên k3s' \
	  '  down         Gỡ Deployment spark-speed-streaming khỏi k3s' \
	  '  status       Xem trạng thái Pod Spark Streaming trên k3s' \
	  '  logs         Xem log trực tiếp của Pod Spark Streaming'

setup:
	"$(PYTHON)" -m pip install -r "$(MODULE_DIR)/requirements.txt"

test:
	cd "$(PROJECT_ROOT)" && SPARK_LOCAL_IP=127.0.0.1 "$(PYTHON)" -m pytest modules/spark_streaming/tests/test_spark_streaming.py -v

sample: run-sample
run-sample:
	cd "$(PROJECT_ROOT)" && SPARK_LOCAL_IP=127.0.0.1 "$(PYTHON)" -m modules.spark_streaming.src.streaming_job --mode sample

kafka: run-kafka
run-kafka:
	cd "$(PROJECT_ROOT)" && SPARK_LOCAL_IP=127.0.0.1 SPARK_KAFKA_PACKAGE="$(KAFKA_PACKAGE)" \
	"$(PYTHON)" -m modules.spark_streaming.src.streaming_job \
	  --mode kafka \
	  --kafka-bootstrap "$(KAFKA_BOOTSTRAP)" \
	  --es-url "$(ES_URL)"

up:
	kubectl apply -f "$(K8S_MANIFEST)"

down:
	kubectl delete -f "$(K8S_MANIFEST)" --ignore-not-found

status:
	kubectl get pods -n "$(NAMESPACE)" -l app=spark-speed-streaming

logs:
	kubectl logs -n "$(NAMESPACE)" -l app=spark-speed-streaming -f

clean:
	rm -f "$(PROJECT_ROOT)/data/test_data/movie_realtime_stats.jsonl"
	rm -f "$(PROJECT_ROOT)/data/test_data/genre_realtime.jsonl"
	rm -f "$(PROJECT_ROOT)/data/test_data/tmdb_realtime.jsonl"
	rm -rf "$(PROJECT_ROOT)/data/test_data/ckpt"
	rm -rf "$(PROJECT_ROOT)/data/test_data/quarantine"
	@printf '%s\n' '[OK] Đã dọn dẹp các file output & checkpoint tạm (giữ nguyên 3 file input mẫu).'
