# Run from the repository root with: make -C modules/data_source <target>
.DEFAULT_GOAL := help
# Shared data artifacts must be processed serially, including under make -j.
.NOTPARALLEL:

MODULE_MAKEFILE := $(abspath $(lastword $(MAKEFILE_LIST)))
PROJECT_ROOT := $(abspath $(dir $(MODULE_MAKEFILE))/../..)
VENV ?= $(PROJECT_ROOT)/.venv
BOOTSTRAP_PYTHON ?= python3
PYTHON ?= $(VENV)/bin/python
PYTEST_ARGS ?= -q
FIXTURE_COUNT ?= 20
SNAPSHOT ?=
INGEST_TIME ?=

RUN = cd "$(PROJECT_ROOT)" && "$(PYTHON)"
SUBMAKE = $(MAKE) --no-print-directory -f "$(MODULE_MAKEFILE)"
FIXTURE_ARGS = --count "$(FIXTURE_COUNT)" $(if $(strip $(SNAPSHOT)),--snapshot "$(SNAPSHOT)") $(if $(strip $(INGEST_TIME)),--ingest-time "$(INGEST_TIME)")

.PHONY: help setup env download download-movielens download-imdb \
        profile build-reference validate-reference sort-ratings tmdb \
        fixtures manifest prepare finish run pipeline verify audit test \
        test-ids test-reference test-movie-event test-tmdb test-contracts

help:
	@printf '%s\n' \
	  'Module 01 - Data Source & Preparation' \
	  'Usage: make -C modules/data_source <target> [VARIABLE=value]' \
	  '' \
	  'Main workflows:' \
	  '  setup               Create venv if missing and install requirements' \
	  '  env                 Create root .env if missing; keep an existing file' \
	  '  pipeline            download -> prepare -> tmdb -> finish (network/API key)' \
	  '  run                 prepare -> finish (existing raw data and snapshot)' \
	  '  verify              Check existing artifacts; refresh fixtures/reports' \
	  '  prepare             profile -> build-reference -> validate-reference -> sort-ratings' \
	  '  finish              fixtures -> manifest -> test -> audit' \
	  '' \
	  'Individual scripts:' \
	  '  download            Reuse complete raw sources or download missing ones' \
	  '  download-movielens   MovieLens downloader; reuse a complete existing source' \
	  '  download-imdb        IMDb downloader; reuse a complete existing source' \
	  '  profile             Profile raw data' \
	  '  build-reference     Build and write canonical reference' \
	  '  validate-reference  Validate canonical reference' \
	  '  sort-ratings        Prepare sorted ratings for replay' \
	  '  tmdb                Fetch one new TMDB snapshot' \
	  '  fixtures            Build three real fixtures' \
	  '  manifest            Verify provenance and update manifest' \
	  '  audit               Audit existing sources and artifacts' \
	  '' \
	  'Tests (offline):' \
	  '  test                All test files' \
	  '  test-ids            test_ids.py' \
	  '  test-reference      test_reference.py' \
	  '  test-movie-event    test_movie_event.py' \
	  '  test-tmdb           test_tmdb_snapshot.py' \
	  '  test-contracts      test_contract_artifacts.py' \
	  '' \
	  'Options: VENV, BOOTSTRAP_PYTHON, PYTHON, PYTEST_ARGS,' \
	  '         FIXTURE_COUNT (20..100), SNAPSHOT, INGEST_TIME' \
	  'Details: modules/data_source/README.md'

setup:
	@if [ ! -x "$(VENV)/bin/python" ]; then "$(BOOTSTRAP_PYTHON)" -m venv "$(VENV)"; fi
	$(RUN) -m pip install -r "$(PROJECT_ROOT)/modules/data_source/requirements.txt"

env:
	@if [ -e "$(PROJECT_ROOT)/.env" ]; then \
	  printf '%s\n' '[SKIP] .env already exists; keeping its contents.'; \
	else \
	  umask 077; \
	  cp "$(PROJECT_ROOT)/modules/data_source/.env.example" "$(PROJECT_ROOT)/.env" && \
	  printf '%s\n' '[OK] .env created. Set TMDB_API_KEY before pipeline/tmdb.'; \
	fi

# Reuse complete raw inputs; the downloaders still reject partial existing data.
# Profiling and manifest perform deeper integrity checks on reused sources.
download-movielens:
	$(RUN) -c 'from modules.data_source.src import download_movielens as d; (d.validate_extracted_dataset(d.DEST_DIR), print("[SKIP] MovieLens raw source already exists.")) if d.DEST_DIR.exists() else d.main()'

download-imdb:
	$(RUN) -c 'from modules.data_source.src import download_imdb as d; print("[SKIP] IMDb raw sources already exist.") if all((d.DEST_DIR / name).is_file() for name in d.REQUIRED_FILES) else d.main()'

download:
	$(SUBMAKE) download-movielens download-imdb

profile:
	$(RUN) -m modules.data_source.src.profile_sources

build-reference:
	$(RUN) -m modules.data_source.src.build_movie_reference

validate-reference:
	$(RUN) -m modules.data_source.src.validate_reference

sort-ratings:
	$(RUN) -m modules.data_source.src.sort_ratings

tmdb:
	$(RUN) -m modules.data_source.src.tmdb_source_adapter --once

fixtures:
	$(RUN) -m modules.data_source.src.build_fixtures $(FIXTURE_ARGS)

manifest:
	$(RUN) -m modules.data_source.src.build_manifest

audit:
	$(RUN) -m modules.data_source.src.audit_module

prepare:
	$(SUBMAKE) profile build-reference validate-reference sort-ratings

finish:
	$(SUBMAKE) fixtures manifest test audit

run:
	$(SUBMAKE) prepare finish

pipeline:
	$(SUBMAKE) download prepare tmdb finish

# Does not rebuild reference, re-sort ratings or call the network.
verify:
	$(SUBMAKE) profile validate-reference fixtures manifest test audit

test:
	$(RUN) -m pytest modules/data_source/tests $(PYTEST_ARGS)

test-ids:
	$(RUN) -m pytest modules/data_source/tests/test_ids.py $(PYTEST_ARGS)

test-reference:
	$(RUN) -m pytest modules/data_source/tests/test_reference.py $(PYTEST_ARGS)

test-movie-event:
	$(RUN) -m pytest modules/data_source/tests/test_movie_event.py $(PYTEST_ARGS)

test-tmdb:
	$(RUN) -m pytest modules/data_source/tests/test_tmdb_snapshot.py $(PYTEST_ARGS)

test-contracts:
	$(RUN) -m pytest modules/data_source/tests/test_contract_artifacts.py $(PYTEST_ARGS)


# Hướng dẫn chạy Makefile
# make -C modules/data_source help        # Xem toàn bộ lệnh
# make -C modules/data_source setup env   # Chuẩn bị môi trường
# make -C modules/data_source pipeline    # Toàn bộ pipeline, cần API key/mạng
# make -C modules/data_source run         # Dựng lại từ nguồn và snapshot local
# make -C modules/data_source verify      # Kiểm tra artifact hiện tại
# make -C modules/data_source test        # Chạy tất cả tests