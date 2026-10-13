#!/usr/bin/env bash
# ==============================================================================
# verify_mapreduce_handoff.sh - Kiểm tra môi trường HDFS cho Module 04 MapReduce
# Môi trường: Kubernetes (k3s)
# Người thực hiện: Giang (Module 03) -> Bàn giao cho Đỗ Sơn (Module 04)
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

echo "=== [MODULE 03 -> MODULE 04] KIỂM TRA MÔI TRƯỜNG CHO HADOOP MAPREDUCE (K8S) ==="
echo "Target Pod: $K8S_POD_NAME (Namespace: $K8S_NAMESPACE)"

if ! kubectl get namespace "$K8S_NAMESPACE" >/dev/null 2>&1; then
    echo -e "\n[!] LỖI KẾT NỐI KUBERNETES:"
    echo "    Namespace '$K8S_NAMESPACE' không tồn tại hoặc cụm Kubernetes chưa chạy."
    echo "    -> Vui lòng chạy lệnh: make -C modules/hdfs up\n"
    exit 1
fi

VERIFY_COMMANDS=$(cat << 'EOF'
echo "1. Kiểm tra Canonical Movie Reference (/movies/ref/current/):"
if hdfs dfs -test -e /movies/ref/current/movie_reference.tsv; then
    echo "  [OK] Đã có /movies/ref/current/movie_reference.tsv"
    echo "  Xem trước 3 dòng đầu:"
    hdfs dfs -cat /movies/ref/current/movie_reference.tsv | head -n 3
else
    echo "  [LƯU Ý] Chưa có file reference! Cần chạy make -C modules/hdfs publish-ref trước."
fi

echo -e "\n2. Kiểm tra Vùng Dữ liệu Thô (Raw Ratings):"
if hdfs dfs -test -e /raw/ratings; then
    echo "  [OK] Thư mục /raw/ratings tồn tại."
    hdfs dfs -ls /raw/ratings || true
else
    echo "  [LƯU Ý] Thư mục /raw/ratings chưa được tạo."
fi

echo -e "\n3. Kiểm tra Quyền ghi vào thư mục Batch Output (/batch):"
if hdfs dfs -test -e /batch; then
    echo "  [OK] Thư mục /batch tồn tại."
    hdfs dfs -ls -d /batch
    echo "  Thử nghiệm tạo thư mục test..."
    hdfs dfs -mkdir -p /batch/_test_permission
    hdfs dfs -rm -r /batch/_test_permission
    echo "  [OK] Phân quyền ghi vào /batch hoàn toàn hoạt động bình thường!"
else
    echo "  [LỖI] Chưa có thư mục /batch!"
fi
EOF
)

echo "$VERIFY_COMMANDS" | kubectl exec -i -n "$K8S_NAMESPACE" "$K8S_POD_NAME" -c namenode -- bash

echo "=== HOÀN TẤT KIỂM TRA CHO MODULE 04! ==="
