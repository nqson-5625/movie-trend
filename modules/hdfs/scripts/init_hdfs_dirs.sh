#!/usr/bin/env bash
# ==============================================================================
# init_hdfs_dirs.sh - Khởi tạo toàn bộ cây thư mục HDFS chuẩn hóa cho Movie-Trend
# Người thực hiện: Giang (Module 03 - HDFS)
# ==============================================================================
set -euo pipefail

CONTAINER_NAME=${1:-hdfs-namenode}

echo "=== [MODULE 03] BẮT ĐẦU KHỞI TẠO CẤU TRÚC THƯ MỤC TRÊN HDFS ==="
echo "Mục tiêu container: $CONTAINER_NAME"

docker exec -i "$CONTAINER_NAME" bash << 'EOF'
set -x

# 1. Tạo các thư mục Vùng Dữ liệu Thô (Raw Zone - Append-only)
hdfs dfs -mkdir -p /raw/ratings
hdfs dfs -mkdir -p /raw/tmdb

# 2. Tạo các thư mục Vùng Cách ly Lỗi (Quarantine Zone)
hdfs dfs -mkdir -p /quarantine/movielens
hdfs dfs -mkdir -p /quarantine/tmdb

# 3. Tạo các thư mục Vùng Tham chiếu Chuẩn (Reference Zone)
hdfs dfs -mkdir -p /movies/ref/current

# 4. Tạo các thư mục Vùng Batch (Hadoop MapReduce Output)
hdfs dfs -mkdir -p /batch/movie_stats
hdfs dfs -mkdir -p /batch/movie_daily_stats
hdfs dfs -mkdir -p /batch/genre_daily_stats
hdfs dfs -mkdir -p /batch/_meta

# 5. Tạo thư mục tạm hệ thống
hdfs dfs -mkdir -p /tmp/hadoop-yarn

# 6. Thiết lập phân quyền chuẩn
hdfs dfs -chmod -R 755 /raw
hdfs dfs -chmod -R 755 /quarantine
hdfs dfs -chmod -R 755 /movies
hdfs dfs -chmod -R 777 /batch
hdfs dfs -chmod -R 777 /tmp

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

echo "=== [MODULE 03] KHỞI TẠO CẤU TRÚC THƯ MỤC THÀNH CÔNG! ==="
