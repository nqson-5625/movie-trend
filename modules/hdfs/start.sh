#!/usr/bin/env bash
# ==============================================================================
# Script điều phối duy nhất cho Module 03 (HDFS Storage Layer trên Kubernetes k3s)
# Tác giả: Giang (Module 03 - HDFS)
# Sử dụng:
#   ./run.sh          (Khởi chạy toàn diện: triển khai K8s, nạp ref, chạy ngầm)
#   ./run.sh status   (Kiểm tra trạng thái cụm và các tiến trình ngầm)
#   ./run.sh stop     (Dừng an toàn các tiến trình ngầm và cụm HDFS)
#   ./run.sh restart  (Khởi động lại toàn bộ hệ thống)
# ==============================================================================

set -o pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
LOG_DIR="${SCRIPT_DIR}/logs"
K8S_DIR="${SCRIPT_DIR}/k8s"
NAMESPACE="bigdata"
NAMENODE_POD="hdfs-namenode-0"

mkdir -p "${LOG_DIR}"

# Định dạng màu sắc thông báo
C_RESET='\033[0m'
C_RED='\033[0;31m'
C_GREEN='\033[0;32m'
C_YELLOW='\033[1;33m'
C_BLUE='\033[0;34m'
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

fatal() {
    printf "\n${C_RED}${C_BOLD}[FATAL ERROR]${C_RESET} %s\n" "$1" >&2
    exit 1
}

# ------------------------------------------------------------------------------
# 1. KIỂM TRA MÔI TRƯỜNG & PHỤ THUỘC (PREREQUISITES)
# ------------------------------------------------------------------------------
check_environment() {
    info "Đang kiểm tra môi trường hệ thống..."

    # Cấu hình KUBECONFIG chuẩn (ưu tiên file readable của user)
    if [ -r "${HOME}/.kube/config" ]; then
        export KUBECONFIG="${HOME}/.kube/config"
    elif [ -r "/etc/rancher/k3s/k3s.yaml" ]; then
        export KUBECONFIG="/etc/rancher/k3s/k3s.yaml"
    elif [ -f "/etc/rancher/k3s/k3s.yaml" ]; then
        sudo chmod 644 /etc/rancher/k3s/k3s.yaml 2>/dev/null || true
        export KUBECONFIG="/etc/rancher/k3s/k3s.yaml"
    fi

    # Kiểm tra lệnh kubectl
    if ! command -v kubectl >/dev/null 2>&1; then
        fatal "Không tìm thấy công cụ 'kubectl'. Vui lòng cài đặt k3s hoặc kubectl trước khi chạy."
    fi

    # Kiểm tra k3s server
    if ! kubectl cluster-info >/dev/null 2>&1; then
        warn "Chưa kết nối được với Kubernetes cluster. Đang thử khởi động k3s..."
        if command -v systemctl >/dev/null 2>&1; then
            sudo systemctl start k3s 2>/dev/null || true
            sleep 5
        fi
        if ! kubectl cluster-info >/dev/null 2>&1; then
            fatal "Không thể kết nối đến Kubernetes Cluster (k3s). Vui lòng kiểm tra lệnh 'systemctl status k3s'."
        fi
    fi

    # Nhận diện Python Virtualenv
    if [ -x "${PROJECT_ROOT}/.venv/bin/python" ]; then
        PYTHON_BIN="${PROJECT_ROOT}/.venv/bin/python"
    elif [ -x "${SCRIPT_DIR}/.venv/bin/python" ]; then
        PYTHON_BIN="${SCRIPT_DIR}/.venv/bin/python"
    elif command -v python3 >/dev/null 2>&1; then
        PYTHON_BIN="$(command -v python3)"
    else
        fatal "Không tìm thấy Python3 để thực thi các script của Module 03."
    fi

    # Đảm bảo file .env tồn tại
    if [ ! -f "${SCRIPT_DIR}/.env" ] && [ -f "${SCRIPT_DIR}/.env.example" ]; then
        cp "${SCRIPT_DIR}/.env.example" "${SCRIPT_DIR}/.env"
        info "Đã tự động khởi tạo file .env từ .env.example"
    fi

    success "Môi trường Kubernetes và Python đã sẵn sàng."
}

# ------------------------------------------------------------------------------
# 2. TRIỂN KHAI CỤM HDFS KUBERNETES
# ------------------------------------------------------------------------------
deploy_k8s_cluster() {
    info "Đang triển khai tài nguyên HDFS lên Kubernetes (Namespace: ${NAMESPACE})..."

    kubectl apply -f "${K8S_DIR}/00-namespace.yaml" >/dev/null 2>&1 || fatal "Lỗi tạo namespace ${NAMESPACE}"
    kubectl apply -f "${K8S_DIR}/01-configmap.yaml" >/dev/null 2>&1 || fatal "Lỗi áp dụng ConfigMap HDFS"
    kubectl apply -f "${K8S_DIR}/02-namenode.yaml" >/dev/null 2>&1 || fatal "Lỗi triển khai StatefulSet NameNode"
    kubectl apply -f "${K8S_DIR}/03-datanode.yaml" >/dev/null 2>&1 || fatal "Lỗi triển khai StatefulSet DataNode"

    info "Đang chờ NameNode (${NAMENODE_POD}) khởi động..."
    if ! kubectl wait --namespace "${NAMESPACE}" --for=condition=ready pod/"${NAMENODE_POD}" --timeout=120s >/dev/null 2>&1; then
        fatal "NameNode pod không thể đạt trạng thái Ready trong 120s! Hãy kiểm tra logs: kubectl logs -n ${NAMESPACE} ${NAMENODE_POD} -c namenode"
    fi

    info "Đang chờ ít nhất 1 DataNode sẵn sàng..."
    if ! kubectl wait --namespace "${NAMESPACE}" --for=condition=ready pod/hdfs-datanode-0 --timeout=120s >/dev/null 2>&1; then
        warn "DataNode 0 chưa Ready hoàn toàn, tiếp tục kiểm tra..."
    fi

    success "Cụm HDFS Kubernetes (NameNode & DataNodes) đã sẵn sàng!"
}

# ------------------------------------------------------------------------------
# 3. KHỞI TẠO CÂY THƯ MỤC & NẠP CANONICAL REFERENCE
# ------------------------------------------------------------------------------
init_hdfs_storage() {
    info "Đang kiểm tra và khởi tạo cấu trúc thư mục chuẩn trên HDFS (/raw, /quarantine, /movies, /batch)..."
    if ! bash "${SCRIPT_DIR}/scripts/init_hdfs_dirs.sh" >/dev/null 2>&1; then
        fatal "Lỗi khởi tạo cây thư mục HDFS qua init_hdfs_dirs.sh"
    fi
    success "Cấu trúc thư mục HDFS chuẩn đã được khởi tạo thành công."

    # Kiểm tra và nạp file Canonical Reference
    local ref_file="${PROJECT_ROOT}/data/prepared/reference/movie_reference.tsv"
    if [ -f "${ref_file}" ]; then
        info "Đang nạp file Canonical Movie Reference (${ref_file}) lên HDFS..."
        if "${PYTHON_BIN}" "${SCRIPT_DIR}/scripts/publish_reference.py" >/dev/null 2>&1; then
            success "Đã nạp thành công Canonical Movie Reference vào /movies/ref/current/"
        else
            warn "Chưa thể nạp Canonical Reference qua script. Kiểm tra file hoặc chạy 'make publish-ref'."
        fi
    else
        warn "Không tìm thấy file ${ref_file}. Bỏ qua bước nạp reference (có thể nạp bổ sung sau)."
    fi
}

# ------------------------------------------------------------------------------
# 4. CHẠY NGẦM CÁC DỊCH VỤ PHỤ TRỢ (BACKGROUND SERVICES)
# ------------------------------------------------------------------------------
start_background_services() {
    info "Đang cấu hình các tiến trình chạy ngầm..."

    # 4.1. Port-forward Web UI (9870) và RPC (9000) với cơ chế tự động phục hồi (Auto-Reconnect)
    if ! ss -tulpn 2>/dev/null | grep -q ":9870 "; then
        nohup bash -c 'while true; do kubectl port-forward --address 0.0.0.0 -n "'"${NAMESPACE}"'" svc/hdfs-namenode-svc 9870:9870 9000:9000 >> "'"${LOG_DIR}"'/port_forward.log" 2>&1; sleep 2; done' >/dev/null 2>&1 &
        local pf_pid=$!
        echo "${pf_pid}" > "${LOG_DIR}/port_forward.pid"
        sleep 2
        success "Đã khởi chạy ngầm Port-Forward tự phục hồi (PID: ${pf_pid}, Web UI: http://localhost:9870)."
    else
        success "Port-forward đã đang mở trên cổng 9870 & 9000."
    fi

    # 4.2. Ingestion Sink Daemon (Lắng nghe Kafka -> Sink HDFS)
    local sink_pid=""
    if [ -f "${LOG_DIR}/kafka_sink.pid" ] && kill -0 "$(cat "${LOG_DIR}/kafka_sink.pid" 2>/dev/null)" 2>/dev/null; then
        sink_pid=$(cat "${LOG_DIR}/kafka_sink.pid")
        success "Kafka-to-HDFS Ingestion Sink Daemon đã đang chạy ngầm (PID: ${sink_pid})."
    elif pgrep -f "kafka_hdfs_sink.py" >/dev/null 2>&1; then
        sink_pid=$(pgrep -f "kafka_hdfs_sink.py" | head -n 1)
        echo "${sink_pid}" > "${LOG_DIR}/kafka_sink.pid"
        success "Kafka-to-HDFS Ingestion Sink Daemon đã đang chạy ngầm (PID: ${sink_pid})."
    else
        nohup "${PYTHON_BIN}" "${SCRIPT_DIR}/scripts/kafka_hdfs_sink.py" --batch-size 200 --flush-interval 15 > "${LOG_DIR}/kafka_sink.log" 2>&1 &
        sink_pid=$!
        echo "${sink_pid}" > "${LOG_DIR}/kafka_sink.pid"
        sleep 2
        if kill -0 "${sink_pid}" 2>/dev/null; then
            success "Đã khởi chạy ngầm Ingestion Sink Daemon (PID: ${sink_pid})."
        else
            warn "Ingestion Sink Daemon không thể khởi động ngầm. Xem log tại ${LOG_DIR}/kafka_sink.log"
        fi
    fi
}

# ------------------------------------------------------------------------------
# 5. XEM TRẠNG THÁI HỆ THỐNG (STATUS)
# ------------------------------------------------------------------------------
show_status() {
    printf "\n${C_BOLD}=== TRẠNG THÁI HỆ THỐNG MODULE 03 (HDFS) ===${C_RESET}\n\n"
    
    info "1. Trạng thái Kubernetes Pods & Services:"
    kubectl get pods,svc,pvc -n "${NAMESPACE}" -l 'app in (hdfs-namenode, hdfs-datanode)'
    
    printf "\n"
    info "2. Trạng thái tiến trình chạy ngầm:"
    if [ -f "${LOG_DIR}/port_forward.pid" ] && kill -0 "$(cat "${LOG_DIR}/port_forward.pid" 2>/dev/null)" 2>/dev/null; then
        printf "  - Port-Forward: ${C_GREEN}ĐANG CHẠY${C_RESET} (PID: %s, Web UI: http://localhost:9870)\n" "$(cat "${LOG_DIR}/port_forward.pid")"
    elif ss -tulpn 2>/dev/null | grep -q ":9870 "; then
        printf "  - Port-Forward: ${C_GREEN}ĐANG CHẠY${C_RESET} (Listening on port 9870 & 9000, Web UI: http://localhost:9870)\n"
    else
        printf "  - Port-Forward: ${C_YELLOW}DỪNG${C_RESET}\n"
    fi

    if [ -f "${LOG_DIR}/kafka_sink.pid" ] && kill -0 "$(cat "${LOG_DIR}/kafka_sink.pid" 2>/dev/null)" 2>/dev/null; then
        printf "  - Ingestion Sink: ${C_GREEN}ĐANG CHẠY${C_RESET} (PID: %s)\n" "$(cat "${LOG_DIR}/kafka_sink.pid")"
    elif pgrep -f "kafka_hdfs_sink.py" >/dev/null 2>&1; then
        printf "  - Ingestion Sink: ${C_GREEN}ĐANG CHẠY${C_RESET} (PID: %s)\n" "$(pgrep -f "kafka_hdfs_sink.py" | head -n 1)"
    else
        printf "  - Ingestion Sink: ${C_YELLOW}DỪNG${C_RESET}\n"
    fi

    printf "\n"
    info "3. Dung lượng HDFS Cluster (dfsadmin -report tóm tắt):"
    kubectl exec -i -n "${NAMESPACE}" pod/"${NAMENODE_POD}" -c namenode -- hdfs dfsadmin -report 2>/dev/null | awk 'NR<=15' || true
}

# ------------------------------------------------------------------------------
# 6. DỪNG HỆ THỐNG AN TOÀN (STOP)
# ------------------------------------------------------------------------------
stop_services() {
    info "Đang dừng các tiến trình chạy ngầm của Module 03..."
    
    if [ -f "${LOG_DIR}/port_forward.pid" ]; then
        kill "$(cat "${LOG_DIR}/port_forward.pid" 2>/dev/null)" 2>/dev/null || true
    fi
    pkill -f "kubectl port-forward.*9870" 2>/dev/null || true
    pkill -f "kubectl port-forward.*${NAMESPACE}" 2>/dev/null || true
    success "Đã dừng Port-Forward."

    if [ -f "${LOG_DIR}/kafka_sink.pid" ]; then
        kill "$(cat "${LOG_DIR}/kafka_sink.pid" 2>/dev/null)" 2>/dev/null || true
    fi
    pkill -f "kafka_hdfs_sink.py" 2>/dev/null || true
    success "Đã dừng Ingestion Sink Daemon."
    
    rm -f "${LOG_DIR}/port_forward.pid" "${LOG_DIR}/kafka_sink.pid"

    if [ "$1" == "--all" ] || [ "$1" == "-a" ]; then
        info "Đang dừng các Pods Kubernetes của HDFS..."
        kubectl delete -f "${K8S_DIR}/03-datanode.yaml" --ignore-not-found >/dev/null 2>&1
        kubectl delete -f "${K8S_DIR}/02-namenode.yaml" --ignore-not-found >/dev/null 2>&1
        success "Đã dừng các StatefulSets HDFS."
    else
        info "Các Pods HDFS trên Kubernetes vẫn được giữ nguyên để sẵn sàng phục vụ. (Dùng './run.sh stop --all' nếu muốn tắt cả Pods)."
    fi
}

# ------------------------------------------------------------------------------
# ENTRYPOINT CHÍNH
# ------------------------------------------------------------------------------
case "${1:-start}" in
    start|up|"")
        printf "\n${C_BOLD}${C_BLUE}================================================================${C_RESET}\n"
        printf "${C_BOLD}${C_BLUE}   KHỞI CHẠY TỰ ĐỘNG MODULE 03: HDFS KUBERNETES & INGESTION    ${C_RESET}\n"
        printf "${C_BOLD}${C_BLUE}================================================================${C_RESET}\n\n"
        
        check_environment
        deploy_k8s_cluster
        init_hdfs_storage
        start_background_services

        printf "\n${C_GREEN}${C_BOLD}================================================================${C_RESET}\n"
        printf "${C_GREEN}${C_BOLD}   MODULE 03 ĐÃ ĐƯỢC KHỞI CHẠY VÀ ĐANG VẬN HÀNH NGẦM HOÀN TẤT!  ${C_RESET}\n"
        printf "${C_GREEN}${C_BOLD}================================================================${C_RESET}\n"
        printf "  * Web UI HDFS:          ${C_CYAN}http://localhost:9870${C_RESET}\n"
        printf "  * RPC NameNode:         ${C_CYAN}hdfs://localhost:9000${C_RESET} (hoặc nội bộ K8s: port 9000)\n"
        printf "  * Canonical Reference:  ${C_CYAN}/movies/ref/current/movie_reference.tsv${C_RESET}\n"
        printf "  * Thư mục Raw Events:   ${C_CYAN}/raw/ratings/dt=YYYY-MM-DD/${C_RESET}\n"
        printf "  * Thư mục Batch Output: ${C_CYAN}/batch/${C_RESET} (Quyền ghi 777 cho MapReduce)\n"
        printf "  * Thư mục File Logs:    ${C_CYAN}${LOG_DIR}/${C_RESET}\n"
        printf "\nLệnh kiểm tra: ${C_YELLOW}./run.sh status${C_RESET} | Lệnh dừng: ${C_YELLOW}./run.sh stop${C_RESET}\n\n"
        exit 0
        ;;
    status|ps)
        check_environment
        show_status
        ;;
    stop|down)
        stop_services "$2"
        ;;
    restart)
        info "Đang khởi động lại hệ thống..."
        stop_services
        sleep 2
        exec "$0" start
        ;;
    *)
        printf "Cách sử dụng: %s [start|status|stop|restart]\n" "$0"
        exit 1
        ;;
esac
