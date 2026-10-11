#!/usr/bin/env bash
# ==============================================================================
# verify_mapreduce_handoff.sh - Kiểm tra môi trường HDFS cho Module 04 MapReduce
# Người thực hiện: Giang (Module 03) -> Bàn giao cho Đỗ Sơn (Module 04)
# ==============================================================================
set -euo pipefail

CONTAINER_NAME=${1:-hdfs-namenode}

echo "=== [MODULE 03 -> MODULE 04] KIỂM TRA MÔI TRƯỜNG CHO HADOOP MAPREDUCE ==="
echo "Container: $CONTAINER_NAME"

docker exec -i "$CONTAINER_NAME" bash << 'EOF'
echo "1. Kiểm tra Canonical Movie Reference (/movies/ref/current/):"
if hdfs dfs -test -e /movies/ref/current/movie_reference.tsv; then
    echo "  [OK] Đã có /movies/ref/current/movie_reference.tsv"
    echo "  Xem trước 3 dòng đầu:"
    hdfs dfs -cat /movies/ref/current/movie_reference.tsv | head -n 3
else
    echo "  [LƯU Ý] Chưa có file reference! Cần chạy python modules/hdfs/scripts/publish_reference.py trước."
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

echo "=== HOÀN TẤT KIỂM TRA CHO MODULE 04! ==="
