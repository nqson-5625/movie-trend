.DEFAULT_GOAL := help

PROJECT_ROOT := $(abspath $(dir $(lastword $(MAKEFILE_LIST)))/../..)
PYTHON ?= $(if $(wildcard $(PROJECT_ROOT)/.venv/bin/python),$(PROJECT_ROOT)/.venv/bin/python,python3)

.PHONY: help validate test test-live smoke

help:
	@echo "Module 07 - Spark SQL shared runtime"
	@echo "  make validate    Validate Phase2-3.0 runtime inventory"
	@echo "  make test        Run offline common tests"
	@echo "  make test-live   Run ES/Spark live tests (requires ES at ES_HTTP_URL)"
	@echo "  make smoke       Start a UTC SparkSession and print runtime versions"

validate:
	cd "$(PROJECT_ROOT)" && "$(PYTHON)" -m modules.spark_sql.runner validate

test:
	cd "$(PROJECT_ROOT)" && "$(PYTHON)" -m pytest -q modules/spark_sql/tests/common

test-live:
	cd "$(PROJECT_ROOT)" && ES_LIVE_TEST=1 "$(PYTHON)" -m pytest -q modules/spark_sql/tests/common

smoke:
	cd "$(PROJECT_ROOT)" && "$(PYTHON)" -m modules.spark_sql.runner smoke
