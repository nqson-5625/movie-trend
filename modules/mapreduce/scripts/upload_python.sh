#!/usr/bin/env bash
# Đưa Python 3 portable lên HDFS (/apps/python/) để các job Hadoop Streaming dùng qua -archives.
# Lý do: image apache/hadoop:3.3.6 (CentOS 7.6) chỉ có Python 2.7. Bản dưới đây đã chạy thử trên CentOS 7 / glibc 2.17.
# Chạy lại an toàn: bỏ qua nếu file đã có trên HDFS với đúng kích thước.
set -euo pipefail

VERSION_TAG="20261003"
FILE="cpython-3.12.15+${VERSION_TAG}-x86_64-unknown-linux-gnu-install_only_stripped.tar.gz"
URL="https://github.com/astral-sh/python-build-standalone/releases/download/${VERSION_TAG}/${FILE//+/%2B}"
SHA256="731af898886c5f821890dc901eca3c651cca8e51fa7308c159d12a1194aeac91"
SIZE=34285590
HDFS_PATH="/apps/python/cpython-3.12.15.tar.gz"

NS=bigdata; POD=yarn-resourcemanager-0; CTR=resourcemanager
export KUBECONFIG="${KUBECONFIG:-$HOME/.kube/config}"
kexec() { kubectl exec -i -n "$NS" "$POD" -c "$CTR" -- "$@"; }

existing=$(kexec hdfs dfs -stat %b "$HDFS_PATH" 2>/dev/null || true)
if [ "$existing" = "$SIZE" ]; then
  echo "[SKIP] $HDFS_PATH đã có trên HDFS ($SIZE bytes)."
  exit 0
fi

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
echo "[INFO] Tải $FILE"
curl -fsSL -o "$tmp/python.tar.gz" "$URL"
echo "$SHA256  $tmp/python.tar.gz" | sha256sum -c - >/dev/null || { echo "[ERROR] Sai SHA-256, dừng."; exit 1; }
echo "[OK] SHA-256 khớp."

kexec sh -c "hdfs dfs -mkdir -p /apps/python && hdfs dfs -put -f - $HDFS_PATH && hdfs dfs -chmod -R 755 /apps" < "$tmp/python.tar.gz"
echo "[OK] Đã đưa lên $HDFS_PATH ($(kexec hdfs dfs -stat %b "$HDFS_PATH" 2>/dev/null) bytes)."
