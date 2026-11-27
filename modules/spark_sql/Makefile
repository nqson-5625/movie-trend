.DEFAULT_GOAL := help

PROJECT_ROOT := $(abspath $(dir $(lastword $(MAKEFILE_LIST)))/../..)
PYTHON ?= $(if $(wildcard $(PROJECT_ROOT)/.venv/bin/python),$(PROJECT_ROOT)/.venv/bin/python,python3)

.PHONY: help validate test test-live test-tmdb smoke tmdb-once tmdb-watch

help:
	@echo "Module 07 - Spark SQL shared runtime"
	@echo "  make validate    Validate Phase2-3.0 runtime inventory"
	@echo "  make test        Run offline common tests"
	@echo "  make test-live   Run ES/Spark live tests (requires ES at ES_HTTP_URL)"
	@echo "  make test-tmdb   Run TMDB realtime lane tests"
	@echo "  make smoke       Start a UTC SparkSession and print runtime versions"
	@echo "  make tmdb-once   Run one sequential TMDB Top-20 sync & publish check"
	@echo "  make tmdb-watch  Watch tmdb_realtime sequentially every 15s"

validate:
	cd "$(PROJECT_ROOT)" && "$(PYTHON)" -m modules.spark_sql.runner validate

test:
	cd "$(PROJECT_ROOT)" && "$(PYTHON)" -m pytest -q modules/spark_sql/tests/common

test-live:
	cd "$(PROJECT_ROOT)" && ES_LIVE_TEST=1 "$(PYTHON)" -m pytest -q modules/spark_sql/tests/common

test-tmdb:
	cd "$(PROJECT_ROOT)" && "$(PYTHON)" -m pytest -v modules/spark_sql/tests/lanes/test_tmdb_realtime.py

smoke:
	cd "$(PROJECT_ROOT)" && "$(PYTHON)" -m modules.spark_sql.runner smoke

tmdb-once:
	cd "$(PROJECT_ROOT)" && "$(PYTHON)" -m modules.spark_sql.lanes.tmdb_realtime.job --once

tmdb-watch:
	cd "$(PROJECT_ROOT)" && "$(PYTHON)" -m modules.spark_sql.lanes.tmdb_realtime.job --loop --poll-interval 15
