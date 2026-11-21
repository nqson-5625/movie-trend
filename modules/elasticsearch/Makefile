.DEFAULT_GOAL := help

NAMESPACE ?= bigdata
K8S_DIR := k8s
ES_POD ?= elasticsearch-0
PROJECT_ROOT := $(abspath $(dir $(lastword $(MAKEFILE_LIST)))/../..)
PYTHON ?= $(if $(wildcard $(PROJECT_ROOT)/.venv/bin/python),$(PROJECT_ROOT)/.venv/bin/python,python3)
REPORTS_DIR := $(PROJECT_ROOT)/data/local/phase2/es_reports
RUN_ID ?=
P2_REPORT ?= $(REPORTS_DIR)/p2_ready_$(RUN_ID).json
P3_REPORT ?= $(REPORTS_DIR)/p3_ready_$(RUN_ID).json
SNAPSHOT ?= $(REPORTS_DIR)/rollback_snapshot_$(RUN_ID).json

.PHONY: help up down status logs forward clean init-templates health test test-live publish-dry publish rollback

help:
	@echo "Module 06 - Elasticsearch (k3s namespace: $(NAMESPACE))"
	@echo "  make -C modules/elasticsearch up              Trien khai ES va cho Ready"
	@echo "  make -C modules/elasticsearch forward         Mo cong 127.0.0.1:9200"
	@echo "  make -C modules/elasticsearch init-templates  Cai dat Component & Index Templates"
	@echo "  make -C modules/elasticsearch health          Kiem tra suc khoe ES & templates"
	@echo "  make -C modules/elasticsearch test            Chay unit test offline"
	@echo "  make -C modules/elasticsearch test-live       Chay tat ca test (can port-forward 9200)"
	@echo "  make -C modules/elasticsearch publish-dry     Kiem tra thu 6 bang Batch (RUN_ID=...)"
	@echo "  make -C modules/elasticsearch publish         Chuyen dong thoi 6 Batch aliases (RUN_ID=...)"
	@echo "  make -C modules/elasticsearch rollback        Hoan tac 6 Batch aliases (RUN_ID=...)"
	@echo "  make -C modules/elasticsearch status          Xem trang thai Pod/Service/PVC"
	@echo "  make -C modules/elasticsearch logs            Xem log container ES"
	@echo "  make -C modules/elasticsearch down            Dung ES (giu lai PVC du lieu)"
	@echo "  make -C modules/elasticsearch clean           Xoa sach ES va ca PVC du lieu"

up:
	kubectl apply -f $(K8S_DIR)/01-elasticsearch-config.yaml
	kubectl apply -f $(K8S_DIR)/02-elasticsearch-statefulset.yaml
	kubectl wait -n $(NAMESPACE) --for=condition=ready pod/$(ES_POD) --timeout=180s
	kubectl get pods,svc,pvc -n $(NAMESPACE) -l app=elasticsearch

status:
	kubectl get pods,svc,pvc -n $(NAMESPACE) -l app=elasticsearch

logs:
	kubectl logs -n $(NAMESPACE) statefulset/elasticsearch -c elasticsearch --tail=100

forward:
	kubectl port-forward --address 127.0.0.1 -n $(NAMESPACE) svc/elasticsearch-svc 9200:9200

down:
	kubectl delete -f $(K8S_DIR)/02-elasticsearch-statefulset.yaml --ignore-not-found
	kubectl delete -f $(K8S_DIR)/01-elasticsearch-config.yaml --ignore-not-found

clean: down
	kubectl delete pvc -n $(NAMESPACE) -l app=elasticsearch --ignore-not-found

init-templates:
	cd "$(PROJECT_ROOT)" && "$(PYTHON)" modules/elasticsearch/common/templates/apply_templates.py

health:
	cd "$(PROJECT_ROOT)" && "$(PYTHON)" -m modules.elasticsearch.common.config.health_check \
		--output "$(PROJECT_ROOT)/data/local/phase2/es_reports/health_check.json"

test:
	cd "$(PROJECT_ROOT)" && "$(PYTHON)" -m pytest -v modules/elasticsearch/tests/common/

test-live:
	cd "$(PROJECT_ROOT)" && ES_LIVE_TEST=1 "$(PYTHON)" -m pytest -v modules/elasticsearch/tests/common/

publish-dry:
	@if [ -z "$(RUN_ID)" ] && [ "$(origin P2_REPORT)" = "file" ]; then \
		echo "Loi: can truyen RUN_ID=<id> (hoac P2_REPORT=... P3_REPORT=...)"; exit 1; \
	fi
	cd "$(PROJECT_ROOT)" && "$(PYTHON)" -m modules.elasticsearch.common.publication.batch_publisher publish \
		--p2-report "$(P2_REPORT)" \
		--p3-report "$(P3_REPORT)" \
		--dry-run

publish:
	@if [ -z "$(RUN_ID)" ] && [ "$(origin P2_REPORT)" = "file" ]; then \
		echo "Loi: can truyen RUN_ID=<id> (hoac P2_REPORT=... P3_REPORT=...)"; exit 1; \
	fi
	cd "$(PROJECT_ROOT)" && "$(PYTHON)" -m modules.elasticsearch.common.publication.batch_publisher publish \
		--p2-report "$(P2_REPORT)" \
		--p3-report "$(P3_REPORT)"

rollback:
	@if [ -z "$(RUN_ID)" ] && [ "$(origin SNAPSHOT)" = "file" ]; then \
		echo "Loi: can truyen RUN_ID=<id> (hoac SNAPSHOT=...)"; exit 1; \
	fi
	cd "$(PROJECT_ROOT)" && "$(PYTHON)" -m modules.elasticsearch.common.publication.batch_publisher rollback \
		--snapshot "$(SNAPSHOT)"