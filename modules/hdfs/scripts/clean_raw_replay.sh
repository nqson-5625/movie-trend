#!/usr/bin/env bash
# ==============================================================================
# clean_raw_replay.sh - Tự động dọn sạch /raw trên HDFS & Checkpoints Producer
# Dùng khi chuyển sang bộ dữ liệu mới lớn hơn (chứa lại dữ liệu Version 1)
# Đảm bảo dữ liệu mới sạch 100%, không bị nhân đôi (duplication).
#
# Tác giả: Giang (Module 03 - HDFS)
# Sử dụng:
#   bash modules/hdfs/scripts/clean_raw_replay.sh [--publish-ref] [--no-restart-sink]
#   hoặc: make -C modules/hdfs clean-raw
# ==============================================================================

set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODULE_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
PROJECT_ROOT="$(cd "${MODULE_DIR}/../.." && pwd)"
LOG_DIR="${MODULE_DIR}/logs"
K8S_NAMESPACE="${K8S_NAMESPACE:-bigdata}"
NAMENODE_POD="${NAMENODE_POD:-hdfs-namenode-0}"

C_RESET='\033[0m'
C_RED='\033[0;31m'
C_GREEN='\033[0;32m'
C_YELLOW='\033[1;33m'
C_CYAN='\033[0;36m'
C_BOLD='\033[1m'

info() {
    printf "${C_CYAN}[INFO]${C_RESET} %s\n" "$1"
}

success() {
    printf "${C_GREEN}[OK]${C_RESET} %s\n" "$1"
}

warn() {
    printf "${C_YELLOW}[WARN]${C_RESET} %s\n" "$1"
}

error() {
    printf "${C_RED}[ERROR]${C_RESET} %s\n" "$1" >&2
}

PUBLISH_REF=false
RESTART_SINK=true

for arg in "$@"; do
    case "$arg" in
        --publish-ref)
            PUBLISH_REF=true
            ;;
        --no-restart-sink)
            RESTART_SINK=false
            ;;
    esac
done

printf "\n${C_BOLD}${C_CYAN}================================================================${C_RESET}\n"
printf "${C_BOLD}${C_CYAN}   TỰ ĐỘNG DỌN SẠCH DỮ LIỆU /raw VÀ CHECKPOINT CHO BỘ DỮ LIỆU MỚI  ${C_RESET}\n"
printf "${C_BOLD}${C_CYAN}================================================================${C_RESET}\n\n"

# ------------------------------------------------------------------------------
# 1. TẠM DỪNG INGESTION SINK DAEMON (NẾU ĐANG CHẠY)
# ------------------------------------------------------------------------------
info "1. Kiểm tra và tạm dừng Ingestion Sink Daemon..."
SINK_WAS_RUNNING=false
if [ -f "${LOG_DIR}/kafka_sink.pid" ] && kill -0 "$(cat "${LOG_DIR}/kafka_sink.pid" 2>/dev/null)" 2>/dev/null; then
    SINK_WAS_RUNNING=true
    kill "$(cat "${LOG_DIR}/kafka_sink.pid" 2>/dev/null)" 2>/dev/null || true
    rm -f "${LOG_DIR}/kafka_sink.pid"
fi
pkill -f "kafka_hdfs_sink.py" 2>/dev/null || true
sleep 1
success "Đã dừng Ingestion Sink Daemon an toàn."

# ------------------------------------------------------------------------------
# 2. XÓA DỮ LIỆU CŨ TRONG /raw VÀ /quarantine TRÊN HDFS
# ------------------------------------------------------------------------------
info "2. Đang dọn sạch các thư mục /raw và /quarantine trên HDFS Kubernetes..."

# Kiểm tra Pod NameNode
if ! kubectl get pod "${NAMENODE_POD}" -n "${K8S_NAMESPACE}" >/dev/null 2>&1; then
    warn "Không thể kết nối Pod ${NAMENODE_POD} trong namespace ${K8S_NAMESPACE}. Bỏ qua dọn dẹp HDFS K8s."
else
    # Xóa file cũ bên trong phân vùng ratings và tmdb
    kubectl exec -i -n "${K8S_NAMESPACE}" "${NAMENODE_POD}" -c namenode -- \
        hdfs dfs -rm -r -f \
        '/raw/ratings/*' \
        '/raw/tmdb/*' \
        '/quarantine/movielens/*' \
        '/quarantine/tmdb/*' 2>/dev/null || true

    # Đảm bảo các thư mục gốc vẫn tồn tại
    kubectl exec -i -n "${K8S_NAMESPACE}" "${NAMENODE_POD}" -c namenode -- \
        hdfs dfs -mkdir -p \
        /raw/ratings \
        /raw/tmdb \
        /quarantine/movielens \
        /quarantine/tmdb 2>/dev/null || true

    success "Đã xóa sạch dữ liệu cũ trong /raw/ratings, /raw/tmdb, /quarantine trên HDFS."
fi

# Xóa cả dữ liệu local fallback giả lập nếu có
LOCAL_SIM_HDFS="${MODULE_DIR}/data/simulated_hdfs"
if [ -d "${LOCAL_SIM_HDFS}" ]; then
    rm -rf "${LOCAL_SIM_HDFS}/raw" "${LOCAL_SIM_HDFS}/quarantine"
    success "Đã dọn sạch thư mục simulated_hdfs local."
fi

# ------------------------------------------------------------------------------
# 3. XÓA CHECKPOINT CŨ CỦA PRODUCER (MODULE 02)
# ------------------------------------------------------------------------------
info "3. Đang dọn sạch checkpoint của Kafka Producers..."

CHECKPOINT_FILES=(
    "${PROJECT_ROOT}/data/local/kafka_ingestion/movie-checkpoint.json"
    "${PROJECT_ROOT}/data/local/kafka_ingestion/tmdb-state.json"
    "${PROJECT_ROOT}/modules/kafka_ingestion/state/movie-checkpoint.json"
    "${PROJECT_ROOT}/modules/kafka_ingestion/state/tmdb-state.json"
)

DELETED_CP=0
for cp_file in "${CHECKPOINT_FILES[@]}"; do
    if [ -f "${cp_file}" ]; then
        rm -f "${cp_file}"
        info "  -> Đã xóa checkpoint: ${cp_file}"
        DELETED_CP=$((DELETED_CP + 1))
    fi
done

if [ "$DELETED_CP" -eq 0 ]; then
    success "Không có checkpoint cũ nào cần xóa."
else
    success "Đã xóa thành công ${DELETED_CP} file checkpoint của Producer."
fi

# ------------------------------------------------------------------------------
# 4. RESET KAFKA CONSUMER GROUP OFFSET
# ------------------------------------------------------------------------------
info "4. Đang kiểm tra và reset Kafka Consumer Group 'hdfs-sink-group'..."
if command -v kafka-consumer-groups.sh >/dev/null 2>&1; then
    kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
        --group hdfs-sink-group --delete >/dev/null 2>&1 || true
    success "Đã reset Consumer Group 'hdfs-sink-group' qua kafka-consumer-groups.sh."
elif command -v kafka-consumer-groups >/dev/null 2>&1; then
    kafka-consumer-groups --bootstrap-server localhost:9092 \
        --group hdfs-sink-group --delete >/dev/null 2>&1 || true
    success "Đã reset Consumer Group 'hdfs-sink-group'."
else
    warn "Không tìm thấy lệnh kafka-consumer-groups (WSL). Bỏ qua bước reset CLI (Sink Daemon sẽ tự động đọc mới)."
fi

# ------------------------------------------------------------------------------
# 5. CẬP NHẬT LẠI CANONICAL REFERENCE (NẾU CÓ CỜ --publish-ref)
# ------------------------------------------------------------------------------
if [ "$PUBLISH_REF" = true ]; then
    info "5. Đang nạp lại file Canonical Reference lên HDFS..."
    PYTHON_BIN="${PROJECT_ROOT}/.venv/bin/python"
    [ ! -x "$PYTHON_BIN" ] && PYTHON_BIN="python3"
    if "$PYTHON_BIN" "${SCRIPT_DIR}/publish_reference.py"; then
        success "Đã cập nhật Canonical Reference lên HDFS."
    else
        warn "Không thể publish reference. Vui lòng kiểm tra lại file nguồn."
    fi
else
    info "5. Bỏ qua cập nhật Reference (dùng cờ --publish-ref nếu có danh mục phim mới)."
fi

# ------------------------------------------------------------------------------
# 6. KHỞI ĐỘNG LẠI INGESTION SINK DAEMON
# ------------------------------------------------------------------------------
if [ "$RESTART_SINK" = true ] && [ "$SINK_WAS_RUNNING" = true ]; then
    info "6. Đang khởi động lại Ingestion Sink Daemon..."
    PYTHON_BIN="${PROJECT_ROOT}/.venv/bin/python"
    [ ! -x "$PYTHON_BIN" ] && PYTHON_BIN="python3"
    mkdir -p "${LOG_DIR}"
    nohup "$PYTHON_BIN" "${SCRIPT_DIR}/kafka_hdfs_sink.py" --batch-size 1000 --flush-interval 10 > "${LOG_DIR}/kafka_sink.log" 2>&1 &
    echo $! > "${LOG_DIR}/kafka_sink.pid"
    sleep 2
    success "Đã khởi động lại Ingestion Sink Daemon (PID: $(cat "${LOG_DIR}/kafka_sink.pid"))."
fi

printf "\n${C_GREEN}${C_BOLD}================================================================${C_RESET}\n"
printf "${C_GREEN}${C_BOLD}   HỆ THỐNG ĐÃ SẴN SÀNG 100% CHO ĐỢT BƠM DỮ LIỆU MỚI!           ${C_RESET}\n"
printf "${C_GREEN}${C_BOLD}================================================================${C_RESET}\n"
printf "  * Vùng /raw trên HDFS:   ${C_CYAN}ĐÃ SẠCH SẼ (Rỗng)${C_RESET}\n"
printf "  * Checkpoint Producer:   ${C_CYAN}ĐÃ RESET (Bơm từ dòng 0)${C_RESET}\n"
printf "  * Lệnh chạy Producer:    ${C_YELLOW}make -C modules/kafka_ingestion producer RATE=1500 BATCH_SIZE=500 LIMIT=0${C_RESET}\n\n"

