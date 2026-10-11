#!/usr/bin/env bash
# ==============================================================================
# init_hdfs_dirs.sh - Khởi tạo toàn bộ cây thư mục HDFS chuẩn hóa cho Movie-Trend
# Môi trường: Kubernetes (k3s)
# Người thực hiện: Giang (Module 03 - HDFS)
# ==============================================================================
set -euo pipefail

# Tự động nạp .env nếu tồn tại
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -f "$SCRIPT_DIR/../.env" ]; then
    set -a; source "$SCRIPT_DIR/../.env"; set +a
elif [ -f "$SCRIPT_DIR/../../../.env" ]; then
    set -a; source "$SCRIPT_DIR/../../../.env"; set +a
fi

K8S_NAMESPACE=${K8S_NAMESPACE:-bigdata}
K8S_POD_NAME=${K8S_POD_NAME:-hdfs-namenode-0}

echo "=== [MODULE 03] KHỞI TẠO CẤU TRÚC THƯ MỤC TRÊN HDFS KUBERNETES ==="
echo "Target Pod: $K8S_POD_NAME (Namespace: $K8S_NAMESPACE)"

if ! kubectl get namespace "$K8S_NAMESPACE" >/dev/null 2>&1; then
    echo -e "\n[!] LỖI KẾT NỐI KUBERNETES:"
    echo "    Namespace '$K8S_NAMESPACE' không tồn tại hoặc cụm Kubernetes chưa chạy."
    echo "    -> Vui lòng chạy lệnh: make -C modules/hdfs up\n"
    exit 1
fi

HDFS_INIT_COMMANDS=$(cat << 'EOF'
set -x

# 1. Tạo các thư mục Vùng Dữ liệu Thô (Raw Zone - Append-only)
hdfs dfs -mkdir -p /raw/ratings
hdfs dfs -mkdir -p /raw/tmdb

# 2. Tạo các thư mục Vùng Cách ly Lỗi (Quarantine Zone)
hdfs dfs -mkdir -p /quarantine/movielens
hdfs dfs -mkdir -p /quarantine/tmdb

# 3. Tạo các thư mục Vùng Tham chiếu Chuẩn (Reference Zone)
hdfs dfs -mkdir -p /movies/ref/current
hdfs dfs -mkdir -p /movies/ref/raw

# 4. Tạo các thư mục Vùng Batch (Hadoop MapReduce Output)
hdfs dfs -mkdir -p /batch/movie_stats
hdfs dfs -mkdir -p /batch/movie_daily_stats
hdfs dfs -mkdir -p /batch/genre_daily_stats
hdfs dfs -mkdir -p /batch/_meta

# 5. Tạo thư mục tạm và checkpoint hệ thống (cho Spark Streaming)
hdfs dfs -mkdir -p /tmp/hadoop-yarn
hdfs dfs -mkdir -p /checkpoint/spark

# 6. Thiết lập phân quyền chuẩn
hdfs dfs -chmod -R 755 /raw
hdfs dfs -chmod -R 755 /quarantine
hdfs dfs -chmod -R 755 /movies
hdfs dfs -chmod -R 777 /batch
hdfs dfs -chmod -R 777 /tmp
hdfs dfs -chmod -R 777 /checkpoint

set +x
echo "=== KIỂM TRA CÂY THƯ MỤC ĐÃ TẠO ==="
hdfs dfs -ls /
echo "--- /raw ---"
hdfs dfs -ls /raw
echo "--- /movies ---"
hdfs dfs -ls /movies
echo "--- /batch ---"
hdfs dfs -ls /batch
EOF
)

echo "$HDFS_INIT_COMMANDS" | kubectl exec -i -n "$K8S_NAMESPACE" pod/"$K8S_POD_NAME" -- bash

echo "=== [MODULE 03] KHỞI TẠO CẤU TRÚC THƯ MỤC THÀNH CÔNG! ==="
