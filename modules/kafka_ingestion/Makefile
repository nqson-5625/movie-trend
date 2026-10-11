ROOT := $(abspath ../..)
PYTHON ?= $(ROOT)/.venv/bin/python
RATE ?= 100
BATCH_SIZE ?= 100
LIMIT ?= 1000
ARGS ?=

.PHONY: help setup start stop status topics topics-check producer replay tmdb consumer test tests integration benchmark kafka-start kafka-stop kafka-status
help:
	@echo 'setup | start | stop | status | topics | topics-check'
	@echo 'producer/replay RATE=100 BATCH_SIZE=100 LIMIT=1000 (0 = full input)'
	@echo 'tmdb | consumer | test/tests | integration | benchmark; extra CLI flags: ARGS="..."'

setup:
	@test -x "$(PYTHON)" || python3 -m venv "$(ROOT)/.venv"
	"$(PYTHON)" -m pip install -r requirements.txt

start: kafka-start
stop: kafka-stop
status: kafka-status
kafka-start:
	cd "$(ROOT)" && "$(PYTHON)" -m modules.kafka_ingestion.src.kafka_lifecycle start
kafka-stop:
	cd "$(ROOT)" && "$(PYTHON)" -m modules.kafka_ingestion.src.kafka_lifecycle stop
kafka-status:
	cd "$(ROOT)" && "$(PYTHON)" -m modules.kafka_ingestion.src.kafka_lifecycle status

topics:
	cd "$(ROOT)" && "$(PYTHON)" -m modules.kafka_ingestion.src.admin_topics apply $(ARGS)
topics-check:
	cd "$(ROOT)" && "$(PYTHON)" -m modules.kafka_ingestion.src.admin_topics check $(ARGS)
producer: replay
replay:
	cd "$(ROOT)" && "$(PYTHON)" -m modules.kafka_ingestion.src.movie_producer --rate $(RATE) --batch-size $(BATCH_SIZE) --limit $(LIMIT) $(ARGS)
tmdb:
	cd "$(ROOT)" && "$(PYTHON)" -m modules.kafka_ingestion.src.tmdb_producer $(ARGS)
consumer:
	cd "$(ROOT)" && "$(PYTHON)" -m modules.kafka_ingestion.src.consumer $(ARGS)
tests: test
test:
	cd "$(ROOT)" && "$(PYTHON)" -m pytest modules/data_source/tests modules/kafka_ingestion/tests -q
integration:
	cd "$(ROOT)" && RUN_KAFKA_INTEGRATION=1 "$(PYTHON)" -m pytest modules/kafka_ingestion/tests/test_integration.py -q $(ARGS)
benchmark:
	cd "$(ROOT)" && "$(PYTHON)" -m modules.kafka_ingestion.src.benchmark $(ARGS)
