# Run from the repository root with: make -C modules/mapreduce <target>
.DEFAULT_GOAL := help
MODULE_MAKEFILE := $(abspath $(lastword $(MAKEFILE_LIST)))
ROOT := $(abspath $(dir $(MODULE_MAKEFILE))/../..)
PYTHON ?= $(ROOT)/.venv/bin/python
PYTEST_ARGS ?= -q
NS := bigdata
export KUBECONFIG ?= $(HOME)/.kube/config

.PHONY: help test yarn-up yarn-down yarn-status python-archive batch download

help:
	@printf '%s\n' \
	  'Module 04 - Hadoop MapReduce (Batch Layer)' \
	  'Usage: make -C modules/mapreduce <target> [VARIABLE=value]' \
	  '' \
	  '  test            Test 3 job bằng pipe mapper|sort|reducer, đối chiếu pandas (offline)' \
	  '  yarn-up         Dựng/bật YARN trên k3s: 1 ResourceManager + 2 NodeManager' \
	  '  yarn-down       Tắt YARN (scale về 0) để trả RAM, giữ cấu hình' \
	  '  yarn-status     Trạng thái pod và NodeManager' \
	  '  python-archive  Đưa Python 3 portable lên HDFS /apps/python (chạy 1 lần)' \
	  '  batch           Chạy 3 job trên YARN: CUTOFF=2018-10-01T00:00:00Z [RUN_ID=...] [JOBS="movie_stats ..."]' \
	  '  download        Tải run mới nhất về data/local/mapreduce/export/ [BATCH_ROOT=/tmp/m4-demo] [RUN_ID=...]'

test:
	cd "$(ROOT)" && "$(PYTHON)" -m pytest modules/mapreduce/tests $(PYTEST_ARGS)

yarn-up:
	kubectl apply -f "$(ROOT)/modules/mapreduce/k8s/"
	kubectl rollout status statefulset/yarn-resourcemanager -n $(NS) --timeout=300s
	kubectl rollout status statefulset/yarn-nodemanager -n $(NS) --timeout=300s

yarn-down:
	kubectl scale statefulset/yarn-nodemanager statefulset/yarn-resourcemanager -n $(NS) --replicas=0

yarn-status:
	kubectl get pods -n $(NS) -l 'app in (yarn-resourcemanager,yarn-nodemanager)'
	kubectl exec -n $(NS) yarn-resourcemanager-0 -c resourcemanager -- yarn node -list 2>/dev/null

python-archive:
	bash "$(ROOT)/modules/mapreduce/scripts/upload_python.sh"

batch:
	CUTOFF="$(CUTOFF)" RUN_ID="$(RUN_ID)" JOBS="$(JOBS)" bash "$(ROOT)/modules/mapreduce/scripts/run_batch.sh"

download:
	RUN_ID="$(RUN_ID)" BATCH_ROOT="$(BATCH_ROOT)" bash "$(ROOT)/modules/mapreduce/scripts/download_batch.sh"
