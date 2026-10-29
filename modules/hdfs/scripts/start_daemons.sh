#!/usr/bin/env bash
set -e

PROJECT_ROOT="/home/giang/movie-trend"
cd "$PROJECT_ROOT"
LOG_DIR="$PROJECT_ROOT/modules/hdfs/logs"
mkdir -p "$LOG_DIR"

pkill -f 'kubectl port-forward' 2>/dev/null || true
pkill -f 'kafka_hdfs_sink.py' 2>/dev/null || true
sleep 1

# Port-forward NameNode (Web UI 9870 & RPC 9000)
nohup bash -c "while true; do kubectl port-forward --address 0.0.0.0 -n bigdata pod/hdfs-namenode-0 9870:9870 9000:9000 >> '$LOG_DIR/port_forward.log' 2>&1; sleep 2; done" >/dev/null 2>&1 &
echo $! > "$LOG_DIR/port_forward.pid"

# Port-forward DataNode 0 (WebHDFS Data transfer 9864)
nohup bash -c "while true; do kubectl port-forward --address 0.0.0.0 -n bigdata pod/hdfs-datanode-0 9864:9864 >> '$LOG_DIR/port_forward_dn.log' 2>&1; sleep 2; done" >/dev/null 2>&1 &
echo $! > "$LOG_DIR/port_forward_dn.pid"

# Ingestion Sink Daemon
nohup "$PROJECT_ROOT/.venv/bin/python" "$PROJECT_ROOT/modules/hdfs/scripts/kafka_hdfs_sink.py" --batch-size 1000 --flush-interval 10 >> "$LOG_DIR/kafka_sink.log" 2>&1 &
echo $! > "$LOG_DIR/kafka_sink.pid"

sleep 2
echo "Daemons started successfully."
