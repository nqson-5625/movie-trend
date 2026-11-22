.DEFAULT_GOAL := help

NAMESPACE ?= bigdata
K8S_DIR := k8s
PROJECT_ROOT := $(abspath $(dir $(lastword $(MAKEFILE_LIST)))/../..)
PYTHON ?= $(if $(wildcard $(PROJECT_ROOT)/.venv/bin/python),$(PROJECT_ROOT)/.venv/bin/python,python3)

.PHONY: help check-secret up down status logs forward validate bootstrap test

help:
	@echo "Module 08 - Kibana (k3s namespace: $(NAMESPACE))"
	@echo "  make -C modules/kibana validate   Validate runtime contract and Data Views"
	@echo "  make -C modules/kibana check-secret Validate required Secret keys without printing values"
	@echo "  make -C modules/kibana up         Deploy Kibana 8.15.3 and wait for Ready"
	@echo "  make -C modules/kibana forward    Forward 127.0.0.1:5601"
	@echo "  make -C modules/kibana bootstrap  Ensure Spaces and canonical Data Views"
	@echo "  make -C modules/kibana status     Show Kibana Deployment and Service"
	@echo "  make -C modules/kibana logs       Show recent Kibana logs"
	@echo "  make -C modules/kibana down       Remove Kibana resources"
	@echo "  make -C modules/kibana test       Run offline infrastructure tests"

validate:
	cd "$(PROJECT_ROOT)" && "$(PYTHON)" -c 'from modules.elasticsearch.common.config.runtime_contract import validate_runtime_contract; from modules.kibana.shared.bootstrap import data_view_definitions, load_contract; print(validate_runtime_contract()); print("Kibana Data Views:", len(data_view_definitions(load_contract())))'
	kubectl kustomize $(K8S_DIR) >/dev/null

check-secret:
	kubectl get secret kibana-credentials -n $(NAMESPACE) -o json | \
	  "$(PYTHON)" "$(PROJECT_ROOT)/modules/kibana/shared/validate_secret.py"

up: check-secret validate
	kubectl apply -k $(K8S_DIR) -n $(NAMESPACE)
	# ConfigMap subPath mounts are immutable in a running pod; force a rollout after apply.
	kubectl rollout restart -n $(NAMESPACE) deployment/kibana
	kubectl rollout status -n $(NAMESPACE) deployment/kibana --timeout=10m

down:
	kubectl delete -k $(K8S_DIR) -n $(NAMESPACE) --ignore-not-found

status:
	kubectl get deployment,pods,svc -n $(NAMESPACE) -l app=kibana

logs:
	kubectl logs -n $(NAMESPACE) deployment/kibana -c kibana --tail=100

forward:
	kubectl port-forward --address 127.0.0.1 -n $(NAMESPACE) svc/kibana-svc 5601:5601

bootstrap:
	cd "$(PROJECT_ROOT)" && "$(PYTHON)" -m modules.kibana.shared.bootstrap

test:
	cd "$(PROJECT_ROOT)" && "$(PYTHON)" -m pytest -v modules/kibana/tests
