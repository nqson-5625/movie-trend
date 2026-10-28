#!/usr/bin/env bash
# Tải Batch View của một run từ HDFS về máy để xem bằng VS Code / Excel:
#   data/local/mapreduce/export/run_id=<run_id>/{manifest.json, <bảng>.tsv}
# File tải về có thêm dòng header lấy từ manifest; bản trên HDFS vẫn là TSV không header.
# (Nút Download trên Web UI 9870 không dùng được từ Windows: NameNode chuyển hướng sang
#  địa chỉ nội bộ của DataNode trong k3s.)
#
#   bash modules/mapreduce/scripts/download_batch.sh                          # run mới nhất trong /batch
#   BATCH_ROOT=/tmp/m4-demo bash modules/mapreduce/scripts/download_batch.sh  # run mới nhất của lần chạy thử
#   RUN_ID=20261024T104145Z bash modules/mapreduce/scripts/download_batch.sh  # một run cụ thể
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
NS=bigdata; POD=hdfs-namenode-0; CTR=namenode
BATCH_ROOT="${BATCH_ROOT:-/batch}"
export KUBECONFIG="${KUBECONFIG:-$HOME/.kube/config}"

die() { echo "[ERROR] $*" >&2; exit 1; }
kexec() { kubectl exec -i -n "$NS" "$POD" -c "$CTR" -- "$@"; }

kubectl get pod -n "$NS" "$POD" >/dev/null 2>&1 || die "HDFS chưa chạy (pod $POD không tồn tại)"
RUN_ID="${RUN_ID:-}"
if [[ -z "$RUN_ID" ]]; then
  # Không truyền RUN_ID: lấy run đã publish mới nhất (run_id dạng YYYYMMDDTHHMMSSZ nên sắp theo tên = theo thời gian)
  RUN_ID=$(kexec hdfs dfs -ls -C "$BATCH_ROOT/_meta" 2>/dev/null | sed -n 's#.*/run_id=##p' | sort | tail -n 1 || true)
  [[ -n "$RUN_ID" ]] || die "Không có run nào đã publish trong $BATCH_ROOT/_meta"
  echo "[INFO] Run mới nhất trong $BATCH_ROOT: run_id=$RUN_ID"
fi

OUT="$ROOT/data/local/mapreduce/export/run_id=$RUN_ID"   # data/local/ đã được gitignore
mkdir -p "$OUT"
# Chỉ tải run đã publish (có manifest), giống quy tắc đọc của Module 06
kexec hdfs dfs -cat "$BATCH_ROOT/_meta/run_id=$RUN_ID/manifest.json" > "$OUT/manifest.json" 2>/dev/null \
  || { rm -rf "$OUT"; die "Không có $BATCH_ROOT/_meta/run_id=$RUN_ID/manifest.json (run chưa publish hoặc sai RUN_ID)"; }

for job in $(python3 -c 'import json, sys; print(" ".join(json.load(open(sys.argv[1]))["jobs"]))' "$OUT/manifest.json"); do
  python3 -c 'import json, sys; print("\t".join(json.load(open(sys.argv[1]))["jobs"][sys.argv[2]]["columns"]))' \
    "$OUT/manifest.json" "$job" > "$OUT/$job.tsv"
  kexec hdfs dfs -cat "$BATCH_ROOT/$job/run_id=$RUN_ID/part-*" >> "$OUT/$job.tsv"
  echo "[OK] $job.tsv: $(( $(wc -l < "$OUT/$job.tsv") - 1 )) dòng"
done

echo "[OK] Đã tải về $OUT"
command -v wslpath >/dev/null 2>&1 && echo "     Mở bằng Windows Explorer: explorer.exe \"$(wslpath -w "$OUT")\""
