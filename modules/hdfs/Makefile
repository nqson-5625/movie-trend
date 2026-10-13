# Run from the repository root with: make -C modules/hdfs <target>
# Or directly from modules/hdfs with: make <target>
.DEFAULT_GOAL := help
.NOTPARALLEL:

MODULE_MAKEFILE := $(abspath $(lastword $(MAKEFILE_LIST)))
MODULE_DIR := $(abspath $(dir $(MODULE_MAKEFILE)))
PROJECT_ROOT := $(abspath $(MODULE_DIR)/../..)
VENV ?= $(PROJECT_ROOT)/.venv
BOOTSTRAP_PYTHON ?= python3

# Tự động nhận diện Python trong virtualenv của repository (Windows & Linux/macOS)
ifeq ($(OS),Windows_NT)
    PYTHON ?= $(if $(wildcard $(VENV)/Scripts/python.exe),$(VENV)/Scripts/python.exe,python)
else
    PYTHON ?= $(if $(wildcard $(VENV)/bin/python),$(VENV)/bin/python,python3)
endif

K8S_DIR := $(MODULE_DIR)/k8s
NAMESPACE ?= bigdata
NAMENODE_POD ?= hdfs-namenode-0

.PHONY: help setup env pipeline verify test run-sink \
        up down restart status ps report init publish-ref verify-mr forward stats clean

help:
	@printf '%s\n' \
	  '================================================================================' \
	  'Module 03 - HDFS Storage Layer (Kubernetes Native — k3s)' \
	  'Usage: make -C modules/hdfs <target> [VARIABLE=value] hoặc cd modules/hdfs && make <target>' \
	  '================================================================================' \
	  '' \
	  'Workflow chính (One-click execution):' \
	  '  setup               Tạo venv (nếu thiếu) và cài đặt thư viện phụ thuộc' \
	  '  env                 Tạo file .env từ .env.example nếu chưa có' \
	  '  pipeline            Triển khai K8s -> Chờ NameNode Ready -> Init dirs -> Publish ref -> Verify' \
	  '  verify              Kiểm tra tính sẵn sàng của cụm HDFS K8s, file reference và phân quyền' \
	  '  test                Chạy kiểm thử tích hợp tự động Module 01 -> Module 03 (offline)' \
	  '  run-sink            Chạy Ingestion Daemon kéo dữ liệu từ Kafka xuống HDFS' \
	  '' \
	  'Điều khiển cụm Kubernetes:' \
	  '  up                  Triển khai cụm HDFS lên Kubernetes namespace: bigdata' \
	  '  down                Dừng cụm HDFS StatefulSets trên Kubernetes' \
	  '  restart             Khởi động lại StatefulSet NameNode & DataNode' \
	  '  status (hoặc ps)    Xem trạng thái Pods, Services, PersistentVolumeClaims' \
	  '  report              Báo cáo dung lượng cụm HDFS (hdfs dfsadmin -report)' \
	  '  init                Khởi tạo cây thư mục chuẩn trên HDFS (/raw, /quarantine, /movies, /batch)' \
	  '  publish-ref         Nghiệm thu Bước 16 (Publish Canonical Reference lên K8s)' \
	  '  verify-mr           Kiểm tra môi trường bàn giao cho Module 04 MapReduce' \
	  '  forward             Port-forward Web UI (9870) và RPC (9000) ra máy host (http://localhost:9870)' \
	  '  stats               Đo lường lượng RAM/CPU tiêu thụ thực tế của các Pods' \
	  '  clean               Dọn dẹp sạch toàn bộ tài nguyên và PersistentVolumeClaims (PVCs)' \
	  '' \
	  'Tài liệu chi tiết: modules/hdfs/docs/K8S_MIGRATION_GUIDE.md'

# ------------------------------------------------------------------------------
# SETUP & MÔI TRƯỜNG
# ------------------------------------------------------------------------------
setup:
	@if [ ! -x "$(VENV)/bin/python" ] && [ ! -x "$(VENV)/Scripts/python.exe" ]; then \
	  "$(BOOTSTRAP_PYTHON)" -m venv "$(VENV)"; \
	fi
	"$(PYTHON)" -m pip install -r "$(MODULE_DIR)/requirements.txt"

env:
	@if [ -e "$(MODULE_DIR)/.env" ] || [ -e "$(PROJECT_ROOT)/.env" ]; then \
	  printf '%s\n' '[SKIP] File .env đã tồn tại. Giữ nguyên nội dung.'; \
	else \
	  cp "$(MODULE_DIR)/.env.example" "$(MODULE_DIR)/.env" && \
	  printf '%s\n' '[OK] Đã tạo file modules/hdfs/.env từ .env.example.'; \
	fi

# ------------------------------------------------------------------------------
# WORKFLOW PIPELINE & VERIFY
# ------------------------------------------------------------------------------
pipeline: up init publish-ref verify-mr
	@printf '%s\n' \
	  '' \
	  '================================================================================' \
	  '  [HOÀN TẤT PIPELINE HDFS KUBERNETES]' \
	  '  - Cụm HDFS (1 NameNode + 2 DataNodes) đang hoạt động ổn định trên Kubernetes.' \
	  '  - Cây thư mục chuẩn đã khởi tạo (/raw, /quarantine, /movies, /batch).' \
	  '  - Dữ liệu tham chiếu chuẩn đã publish tại: /movies/ref/current/movie_reference.tsv' \
	  '  - Môi trường đã sẵn sàng bàn giao cho Module 04 MapReduce & Module 05 Spark!' \
	  '================================================================================'

verify:
	@printf '%s\n' '=== KIỂM TRA SỨC KHỎE CỤM HDFS KUBERNETES ==='
	$(MAKE) --no-print-directory -f "$(MODULE_MAKEFILE)" status
	$(MAKE) --no-print-directory -f "$(MODULE_MAKEFILE)" verify-mr
	$(MAKE) --no-print-directory -f "$(MODULE_MAKEFILE)" test

# ------------------------------------------------------------------------------
# ĐIỀU KHIỂN CỤM KUBERNETES
# ------------------------------------------------------------------------------
up:
	@printf '%s\n' 'Đang triển khai cụm HDFS lên Kubernetes namespace $(NAMESPACE)...'
	kubectl apply -f "$(K8S_DIR)/00-namespace.yaml"
	kubectl apply -f "$(K8S_DIR)/01-configmap.yaml"
	kubectl apply -f "$(K8S_DIR)/02-namenode.yaml"
	kubectl apply -f "$(K8S_DIR)/03-datanode.yaml"
	@printf '%s\n' 'Đang chờ NameNode Pod ($(NAMENODE_POD)) sẵn sàng...'
	@kubectl wait --namespace $(NAMESPACE) --for=condition=ready pod/$(NAMENODE_POD) --timeout=180s || true
	@printf '%s\n' '[OK] Triển khai thành công. Chạy "make status" để xem trạng thái.'

down:
	@printf '%s\n' 'Dừng cụm HDFS trên Kubernetes...'
	kubectl delete -f "$(K8S_DIR)/03-datanode.yaml" --ignore-not-found
	kubectl delete -f "$(K8S_DIR)/02-namenode.yaml" --ignore-not-found
	kubectl delete -f "$(K8S_DIR)/01-configmap.yaml" --ignore-not-found

restart:
	kubectl rollout restart statefulset/hdfs-namenode -n $(NAMESPACE)
	kubectl rollout restart statefulset/hdfs-datanode -n $(NAMESPACE)

status:
	kubectl get pods,svc,pvc -n $(NAMESPACE) -l 'app in (hdfs-namenode, hdfs-datanode)'

ps: status

report:
	kubectl exec -i -n $(NAMESPACE) pod/$(NAMENODE_POD) -- hdfs dfsadmin -report

init:
	bash "$(MODULE_DIR)/scripts/init_hdfs_dirs.sh"

publish-ref:
	"$(PYTHON)" "$(MODULE_DIR)/scripts/publish_reference.py"

verify-mr:
	bash "$(MODULE_DIR)/scripts/verify_mapreduce_handoff.sh"

forward:
	@printf '%s\n' 'Forwarding Web UI (9870) và RPC (9000) từ K8s tới localhost...'
	kubectl port-forward -n $(NAMESPACE) svc/hdfs-namenode-svc 9870:9870 9000:9000

stats:
	kubectl top pods -n $(NAMESPACE) -l 'app in (hdfs-namenode, hdfs-datanode)'

clean:
	@printf '%s\n' 'Xóa toàn bộ tài nguyên và PVCs của HDFS trên Kubernetes...'
	kubectl delete -f "$(K8S_DIR)/" --ignore-not-found
	kubectl delete pvc -l 'app in (hdfs-namenode, hdfs-datanode)' -n $(NAMESPACE) --ignore-not-found
	@printf '%s\n' '[OK] Đã dọn dẹp sạch sẽ.'

# ------------------------------------------------------------------------------
# KIỂM THỬ & TIỆN ÍCH
# ------------------------------------------------------------------------------
test:
	"$(PYTHON)" "$(MODULE_DIR)/tests/test_flow_module_1_to_3.py"
	"$(PYTHON)" "$(MODULE_DIR)/tests/test_kafka_hdfs_sink.py"

test-sink:
	"$(PYTHON)" "$(MODULE_DIR)/tests/test_kafka_hdfs_sink.py"

sink: run-sink

run-sink:
	"$(PYTHON)" "$(MODULE_DIR)/scripts/kafka_hdfs_sink.py"
