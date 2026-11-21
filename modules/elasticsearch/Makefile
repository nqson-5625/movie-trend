.DEFAULT_GOAL := help

NAMESPACE ?= bigdata
K8S_DIR := k8s
ES_POD ?= elasticsearch-0

.PHONY: help up down status logs forward clean

help:
	@echo "Module 06 - Elasticsearch (k3s namespace: $(NAMESPACE))"
	@echo "  make -C modules/elasticsearch up       Trien khai ES va cho Ready"
	@echo "  make -C modules/elasticsearch forward  Mo cong 127.0.0.1:9200"
	@echo "  make -C modules/elasticsearch status   Xem trang thai Pod/Service/PVC"
	@echo "  make -C modules/elasticsearch logs     Xem log container ES"
	@echo "  make -C modules/elasticsearch down     Dung ES (giu lai PVC du lieu)"
	@echo "  make -C modules/elasticsearch clean    Xoa sach ES va ca PVC du lieu"

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