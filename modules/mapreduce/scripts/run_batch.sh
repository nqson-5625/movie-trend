#!/usr/bin/env bash
# Chạy Batch Layer (Module 04) trên YARN: 3 job Hadoop Streaming dùng chung một run_id,
# rồi ghi manifest vào /batch/_meta/run_id=<run_id>/manifest.json (contract mục 9.3).
#
#   CUTOFF=2018-10-01T00:00:00Z bash modules/mapreduce/scripts/run_batch.sh
#   CUTOFF=... JOBS="movie_stats" RUN_ID=20261024T120000Z bash modules/mapreduce/scripts/run_batch.sh
#
# Batch View chỉ tính event có event_time < CUTOFF (contract mục 5.4). CUTOFF bắt buộc, UTC, tròn giờ.
# Output: /batch/<job>/run_id=<run_id>/part-NNNNN, TSV không header, thứ tự cột ghi trong manifest.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
SRC="$ROOT/modules/mapreduce/src"
NS=bigdata; POD=yarn-resourcemanager-0; CTR=resourcemanager
STREAMING_JAR=/opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar
PYTHON_ARCHIVE=/apps/python/cpython-3.12.15.tar.gz
REFERENCE=/movies/ref/current/movie_reference.tsv
INPUT='/raw/ratings/dt=*/*.jsonl'
ALL_JOBS="movie_stats movie_daily_stats genre_daily_stats"
export KUBECONFIG="${KUBECONFIG:-$HOME/.kube/config}"

die() { echo "[ERROR] $*" >&2; exit 1; }
kexec() { kubectl exec -i -n "$NS" "$POD" -c "$CTR" -- "$@"; }
# Run thất bại: xóa output dở dang của CHÍNH run này (chưa có manifest nên chưa ai được đọc), rồi dừng.
fail_run() {
  echo "[ERROR] $*" >&2
  for j in $JOBS; do kexec hdfs dfs -rm -r -f -skipTrash "/batch/$j/run_id=$RUN_ID" >/dev/null 2>&1 || true; done
  echo "[INFO] Đã xóa output dở dang của run_id=$RUN_ID" >&2
  exit 1
}

CUTOFF="${CUTOFF:-}"
[[ "$CUTOFF" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:00:00Z$ ]] || die "CUTOFF phải là UTC tròn giờ, ví dụ CUTOFF=2018-10-01T00:00:00Z"
GENERATED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
RUN_ID="${RUN_ID:-$(echo "$GENERATED_AT" | tr -d ':-')}"
JOBS="${JOBS:-$ALL_JOBS}"
for job in $JOBS; do [[ " $ALL_JOBS " == *" $job "* ]] || die "Job không hợp lệ: $job"; done
LOG_DIR="$ROOT/data/local/mapreduce/run_id=$RUN_ID"   # data/local/ đã được gitignore
mkdir -p "$LOG_DIR"

echo "[INFO] run_id=$RUN_ID cutoff=$CUTOFF generated_at=$GENERATED_AT jobs=$JOBS"

# Kiểm tra trước khi chạy
kubectl get pod -n "$NS" "$POD" >/dev/null 2>&1 || die "YARN chưa chạy (make -C modules/mapreduce yarn-up)"
kexec hdfs dfs -test -e "$PYTHON_ARCHIVE" 2>/dev/null || die "Thiếu $PYTHON_ARCHIVE (make -C modules/mapreduce python-archive)"
kexec hdfs dfs -test -e "$REFERENCE" 2>/dev/null || die "Thiếu $REFERENCE (Module 03 chưa publish reference)"
kexec hdfs dfs -test -e "/batch/_meta/run_id=$RUN_ID" 2>/dev/null && die "run_id=$RUN_ID đã được publish (contract 16.4: không publish lại)"
for job in $JOBS; do
  kexec hdfs dfs -test -e "/batch/$job/run_id=$RUN_ID" 2>/dev/null && die "/batch/$job/run_id=$RUN_ID đã tồn tại"
done

# Gửi code vào pod gửi job
WORK="/tmp/m4job/$RUN_ID"
trap 'kexec rm -rf "$WORK" >/dev/null 2>&1 || true' EXIT
kexec mkdir -p "$WORK"
for f in "$SRC"/*.py; do kubectl cp "$f" "$NS/$POD:$WORK/$(basename "$f")" -c "$CTR"; done

for job in $JOBS; do
  out="/batch/$job/run_id=$RUN_ID"
  echo "[INFO] Job $job -> $out"
  # -D, -files, -archives (generic options) phải đứng trước các option của Streaming.
  kexec sh -c "cd $WORK && hadoop jar $STREAMING_JAR \
    -D mapreduce.job.name=m4-$job-$RUN_ID \
    -D mapreduce.job.reduces=2 \
    -D mapreduce.input.fileinputformat.split.maxsize=8388608 \
    -D stream.map.input.ignoreKey=true \
    -files mr_common.py,${job}_mapper.py,${job}_reducer.py,hdfs://$REFERENCE#movie_reference.tsv \
    -archives hdfs://$PYTHON_ARCHIVE#py \
    -inputformat org.apache.hadoop.mapred.lib.CombineTextInputFormat \
    -input '$INPUT' \
    -output '$out' \
    -mapper 'py/python/bin/python3 ${job}_mapper.py' \
    -reducer 'py/python/bin/python3 ${job}_reducer.py' \
    -cmdenv PYTHONPATH=. \
    -cmdenv BATCH_CUTOFF_TIME=$CUTOFF -cmdenv BATCH_RUN_ID=$RUN_ID -cmdenv GENERATED_AT=$GENERATED_AT" \
    > "$LOG_DIR/$job.log" 2>&1 || fail_run "Job $job lỗi, xem $LOG_DIR/$job.log"
  grep -q "completed successfully" "$LOG_DIR/$job.log" || fail_run "Job $job không hoàn tất, xem $LOG_DIR/$job.log"
done

# Kiểm tra counters và dựng manifest (ghi sau cùng: có manifest = run đã hoàn tất)
REF_VERSION=$(kexec sh -c "hdfs dfs -cat $REFERENCE 2>/dev/null | sed -n 2p | cut -f9")
python3 "$ROOT/modules/mapreduce/scripts/build_manifest.py" \
  --log-dir "$LOG_DIR" --jobs $JOBS --run-id "$RUN_ID" --cutoff "$CUTOFF" --generated-at "$GENERATED_AT" \
  --input "$INPUT" --reference "$REFERENCE" --ref-version "$REF_VERSION" --python-archive "$PYTHON_ARCHIVE" \
  > "$LOG_DIR/manifest.json" || fail_run "Counters bất thường, xem $LOG_DIR/*.log"
kexec sh -c "hdfs dfs -mkdir -p /batch/_meta/run_id=$RUN_ID && hdfs dfs -put - /batch/_meta/run_id=$RUN_ID/manifest.json" < "$LOG_DIR/manifest.json"
echo "[OK] Đã publish /batch/_meta/run_id=$RUN_ID/manifest.json (bản sao: $LOG_DIR/manifest.json)"
