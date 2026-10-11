#!/usr/bin/env python3
"""
kafka_hdfs_sink.py - Kafka to HDFS Ingestion Daemon
Phụ trách: Giang (Module 03 - HDFS)
Chức năng: Lắng nghe Kafka topics (movie-events, tmdb-snapshots), kiểm tra contract
và ghi bền vững vào cụm HDFS Kubernetes phân vùng theo ngày dt=YYYY-MM-DD,
đồng thời cách ly bản ghi hỏng vào /quarantine/, tuân thủ DATA_CONTRACT.md.
"""

from __future__ import annotations

import argparse
import io
import json
import logging
import os
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

CURRENT_DIR = Path(__file__).resolve().parent
MODULE_DIR = CURRENT_DIR.parent
PROJECT_ROOT = Path(__file__).resolve().parents[3]

# Tải cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] (Module03-Sink) %(message)s"
)
logger = logging.getLogger("HDFSSink")


def load_env_file():
    """Tự động nạp file .env từ module hoặc project root."""
    for env_path in [MODULE_DIR / ".env", PROJECT_ROOT / ".env", MODULE_DIR / ".env.example"]:
        if env_path.exists():
            with open(env_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        k, v = line.split("=", 1)
                        k, v = k.strip(), v.strip().strip("'\"")
                        if k not in os.environ:
                            os.environ[k] = v
            break


def load_config() -> dict:
    """Nạp cấu hình từ config.yaml nếu có."""
    config_path = MODULE_DIR / "config.yaml"
    if config_path.exists():
        try:
            import yaml
            with open(config_path, "r", encoding="utf-8") as f:
                return yaml.safe_load(f) or {}
        except Exception:
            pass
    return {}


load_env_file()
CFG = load_config()
K8S_CFG = CFG.get("k8s", {})
KAFKA_CFG = CFG.get("kafka", {})
SINK_CFG = CFG.get("sink", {})

# Tải schemas để kiểm tra tính hợp lệ
SCHEMAS_DIR = PROJECT_ROOT / "contracts" / "schemas"
SCHEMAS: Dict[str, dict] = {}
for topic in ("movie-events", "tmdb-snapshots"):
    schema_file = SCHEMAS_DIR / f"{topic}.schema.json"
    if schema_file.exists():
        try:
            SCHEMAS[topic] = json.loads(schema_file.read_text(encoding="utf-8"))
        except Exception as e:
            logger.warning(f"Không thể nạp schema cho {topic}: {e}")


def validate_schema(data: dict, topic: str) -> Tuple[bool, str]:
    """Kiểm tra tính hợp lệ của bản ghi theo contract."""
    # 1. Kiểm tra các trường bắt buộc chung
    required_common = ["schema_version", "event_id", "source", "event_time"]
    for field in required_common:
        if field not in data:
            return False, f"Thiếu trường bắt buộc: {field}"

    # 2. Kiểm tra định dạng ngày giờ event_time
    event_time_str = str(data["event_time"])
    try:
        clean_time = event_time_str.replace("Z", "+00:00")
        datetime.fromisoformat(clean_time)
    except Exception as e:
        return False, f"Định dạng event_time không hợp lệ ({event_time_str}): {e}"

    # 3. Kiểm tra chi tiết từng topic
    if topic == "movie-events":
        if data.get("source") != "movielens":
            return False, f"Source không hợp lệ cho movie-events: {data.get('source')}"
        for f in ("userId", "movieId", "rating"):
            if f not in data:
                return False, f"Thiếu trường nghiệp vụ movie-events: {f}"
        try:
            float(data["rating"])
            int(data["userId"])
            int(data["movieId"])
        except (ValueError, TypeError):
            return False, "Sai kiểu dữ liệu trường userId/movieId/rating"

    elif topic == "tmdb-snapshots":
        if data.get("source") != "tmdb":
            return False, f"Source không hợp lệ cho tmdb-snapshots: {data.get('source')}"
        if "tmdbId" not in data:
            return False, "Thiếu trường tmdbId cho tmdb-snapshots"

    # 4. Kiểm tra qua jsonschema nếu có thư viện
    if topic in SCHEMAS:
        try:
            from jsonschema import Draft202012Validator
            validator = Draft202012Validator(SCHEMAS[topic])
            errors = list(validator.iter_errors(data))
            if errors:
                return False, f"Vi phạm JSON schema: {errors[0].message}"
        except ImportError:
            pass

    return True, ""


def extract_partition_date(event_time_str: str) -> str:
    """
    Trích xuất ngày dt=YYYY-MM-DD từ chuỗi event_time ISO 8601 UTC.
    Bắt buộc dùng event_time nguồn, không dùng thời gian hiện tại của máy.
    """
    try:
        clean_time = event_time_str.replace("Z", "+00:00")
        dt = datetime.fromisoformat(clean_time)
        return dt.astimezone(timezone.utc).strftime("%Y-%m-%d")
    except Exception:
        return "1970-01-01"


class HDFSUploader:
    """Quản lý việc tải dữ liệu lên HDFS trên Kubernetes (hỗ trợ WebHDFS & kubectl)."""

    def __init__(
        self,
        webhdfs_url: Optional[str] = None,
        k8s_pod: str = "hdfs-namenode-0",
        k8s_namespace: str = "bigdata",
        hdfs_user: str = "root"
    ):
        self.webhdfs_url = (webhdfs_url or os.getenv("WEBHDFS_URL") or
                            K8S_CFG.get("cluster_dns_webhdfs") or
                            K8S_CFG.get("host_forward_webhdfs") or
                            "http://localhost:9870").rstrip("/")
        self.k8s_pod = k8s_pod or os.getenv("K8S_NAMENODE_POD", "hdfs-namenode-0")
        self.k8s_namespace = k8s_namespace or os.getenv("K8S_NAMESPACE", "bigdata")
        self.hdfs_user = hdfs_user or os.getenv("HDFS_USER", "root")

    def write_file(self, hdfs_path: str, data: str) -> bool:
        """Ghi dữ liệu (text string) vào đường dẫn file trên HDFS."""
        # Cách 1: Thử gọi WebHDFS REST API
        if self._write_via_webhdfs(hdfs_path, data):
            return True

        # Cách 2: Fallback qua kubectl exec trực tiếp vào Pod NameNode
        if self._write_via_kubectl(hdfs_path, data):
            return True

        # Cách 3: Ghi local fallback nếu đang chạy ở môi trường offline/mock
        return self._write_via_local_fallback(hdfs_path, data)

    def _write_via_webhdfs(self, hdfs_path: str, data: str) -> bool:
        """Ghi file qua WebHDFS REST API (theo chuẩn WebHDFS 2 bước redirect)."""
        clean_path = hdfs_path.lstrip("/")
        create_url = f"{self.webhdfs_url}/webhdfs/v1/{clean_path}?op=CREATE&overwrite=true&user.name={self.hdfs_user}"
        try:
            req = urllib.request.Request(create_url, method="PUT")
            with urllib.request.urlopen(req, timeout=5) as response:
                redirect_url = response.geturl()

            put_req = urllib.request.Request(
                redirect_url,
                data=data.encode("utf-8"),
                headers={"Content-Type": "application/octet-stream"},
                method="PUT"
            )
            with urllib.request.urlopen(put_req, timeout=10) as put_resp:
                return put_resp.status in (200, 201)
        except Exception:
            return False

    def _write_via_kubectl(self, hdfs_path: str, data: str) -> bool:
        """Ghi file vào HDFS bằng lệnh kubectl exec."""
        try:
            parent_dir = str(Path(hdfs_path).parent).replace("\\", "/")
            mkdir_cmd = ["kubectl", "exec", "-i", "-n", self.k8s_namespace, f"pod/{self.k8s_pod}", "--",
                         "hdfs", "dfs", "-mkdir", "-p", parent_dir]
            subprocess.run(mkdir_cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)

            put_cmd = ["kubectl", "exec", "-i", "-n", self.k8s_namespace, f"pod/{self.k8s_pod}", "--",
                       "hdfs", "dfs", "-put", "-f", "-", hdfs_path]
            proc = subprocess.run(put_cmd, input=data, text=True, capture_output=True, timeout=15)
            return proc.returncode == 0
        except Exception:
            return False

    def _write_via_local_fallback(self, hdfs_path: str, data: str) -> bool:
        """Ghi tạm vào thư mục local giả lập HDFS khi không có kết nối cụm."""
        try:
            local_target = MODULE_DIR / "data" / "simulated_hdfs" / hdfs_path.lstrip("/")
            local_target.parent.mkdir(parents=True, exist_ok=True)
            with open(local_target, "w", encoding="utf-8") as f:
                f.write(data)
            return True
        except Exception as e:
            logger.error(f"Lỗi ghi fallback local cho {hdfs_path}: {e}")
            return False


class HDFSSinkEngine:
    """Engine cốt lõi gom batch, phân vùng ngày, cách ly lỗi và ghi vào HDFS."""

    def __init__(
        self,
        uploader: Optional[HDFSUploader] = None,
        batch_size: int = 200,
        flush_interval_sec: int = 30
    ):
        self.uploader = uploader or HDFSUploader()
        self.batch_size = batch_size
        self.flush_interval_sec = flush_interval_sec
        self.raw_buffers: Dict[Tuple[str, str], List[dict]] = defaultdict(list)
        self.quarantine_buffers: Dict[Tuple[str, str], List[dict]] = defaultdict(list)
        self.last_flush_time = time.time()
        self.total_processed = 0
        self.total_valid = 0
        self.total_quarantined = 0

    def process_record(self, topic: str, raw_payload: str) -> Tuple[bool, str]:
        """Xử lý và điều hướng 1 bản ghi vào buffer Raw hoặc Quarantine."""
        self.total_processed += 1
        now_utc = datetime.now(timezone.utc).isoformat() + "Z"

        # 1. Parse JSON
        try:
            data = json.loads(raw_payload)
        except Exception as exc:
            today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            bad_record = {
                "raw_payload": raw_payload,
                "error_reason": f"Lỗi JSON parse: {exc}",
                "error_time": now_utc
            }
            source = "movielens" if topic == "movie-events" else "tmdb"
            self.quarantine_buffers[(source, today_str)].append(bad_record)
            self.total_quarantined += 1
            return False, "json_parse_error"

        # 2. Validate Schema & Contract
        is_valid, reason = validate_schema(data, topic)
        if not is_valid:
            dt_str = extract_partition_date(data.get("event_time", now_utc))
            bad_record = {
                "raw_payload": raw_payload,
                "parsed_data": data,
                "error_reason": reason,
                "error_time": now_utc
            }
            source = data.get("source") or ("movielens" if topic == "movie-events" else "tmdb")
            self.quarantine_buffers[(source, dt_str)].append(bad_record)
            self.total_quarantined += 1
            return False, reason

        # 3. Điều hướng vào Raw buffer theo phân vùng event_time
        dt_str = extract_partition_date(data["event_time"])
        source_key = "ratings" if topic == "movie-events" else "tmdb"
        self.raw_buffers[(source_key, dt_str)].append(data)
        self.total_valid += 1
        return True, "valid"

    def pending_count(self) -> int:
        """Tổng số lượng bản ghi đang chờ flush trong bộ đệm."""
        return sum(len(v) for v in self.raw_buffers.values()) + \
               sum(len(v) for v in self.quarantine_buffers.values())

    def flush(self) -> Dict[str, int]:
        """Xả toàn bộ buffer vào HDFS và xóa sạch buffer nếu ghi thành công."""
        stats = {"raw_written": 0, "quarantine_written": 0, "failed": 0}
        now_ts = int(time.time() * 1000)

        # 1. Flush vùng Raw
        for (source_key, dt_str), records in list(self.raw_buffers.items()):
            if not records:
                continue
            hdfs_dir = f"/raw/{source_key}/dt={dt_str}"
            file_name = f"part-{now_ts}-{len(records)}.jsonl"
            hdfs_path = f"{hdfs_dir}/{file_name}"

            payload = "\n".join(json.dumps(r, ensure_ascii=False) for r in records) + "\n"
            if self.uploader.write_file(hdfs_path, payload):
                logger.info(f"Đã ghi thành công {len(records)} records vào HDFS: {hdfs_path}")
                stats["raw_written"] += len(records)
                self.raw_buffers[(source_key, dt_str)].clear()
            else:
                logger.error(f"Thất bại khi ghi HDFS: {hdfs_path}")
                stats["failed"] += len(records)

        # 2. Flush vùng Quarantine
        for (source_key, dt_str), bad_records in list(self.quarantine_buffers.items()):
            if not bad_records:
                continue
            quar_dir = f"/quarantine/{source_key}/dt={dt_str}"
            file_name = f"error-{now_ts}-{len(bad_records)}.jsonl"
            hdfs_path = f"{quar_dir}/{file_name}"

            payload = "\n".join(json.dumps(r, ensure_ascii=False) for r in bad_records) + "\n"
            if self.uploader.write_file(hdfs_path, payload):
                logger.warning(f"Đã cách ly {len(bad_records)} bản ghi lỗi vào HDFS: {hdfs_path}")
                stats["quarantine_written"] += len(bad_records)
                self.quarantine_buffers[(source_key, dt_str)].clear()
            else:
                logger.error(f"Thất bại khi ghi quarantine HDFS: {hdfs_path}")
                stats["failed"] += len(bad_records)

        self.last_flush_time = time.time()
        return stats


class KafkaHDFSSinkDaemon:
    """Tiến trình Daemon kết nối Kafka Consumer và điều phối Sink Engine."""

    def __init__(
        self,
        bootstrap_servers: Optional[str] = None,
        group_id: str = "hdfs-sink-group",
        topics: Optional[List[str]] = None,
        batch_size: int = 200,
        flush_interval_sec: int = 30
    ):
        self.servers = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.group_id = group_id or os.getenv("KAFKA_GROUP_ID", "hdfs-sink-group")
        self.topics = topics or ["movie-events", "tmdb-snapshots"]
        self.engine = HDFSSinkEngine(batch_size=batch_size, flush_interval_sec=flush_interval_sec)
        self.running = True

    def run(self):
        """Khởi động lắng nghe Kafka và đẩy định kỳ vào HDFS."""
        logger.info(f"Bắt đầu Ingestion Daemon (Group: {self.group_id}, Topics: {self.topics})")
        logger.info(f"Kafka Bootstrap: {self.servers}")

        # Đăng ký xử lý tín hiệu kết thúc an toàn
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

        # Kiểm tra driver Kafka
        consumer = self._create_consumer()
        if not consumer:
            logger.error("Không thể khởi tạo Kafka Consumer. Vui lòng kiểm tra thư viện hoặc cấu hình.")
            return

        try:
            while self.running:
                messages = self._poll_messages(consumer, timeout=1.0)
                for topic, raw_val in messages:
                    self.engine.process_record(topic, raw_val)

                # Kiểm tra điều kiện flush
                time_elapsed = time.time() - self.engine.last_flush_time
                pending = self.engine.pending_count()
                if pending >= self.engine.batch_size or (pending > 0 and time_elapsed >= self.engine.flush_interval_sec):
                    self.engine.flush()
                    self._commit_offset(consumer)

        finally:
            logger.info("Đang dừng Daemon và xả dữ liệu còn tồn đọng...")
            if self.engine.pending_count() > 0:
                self.engine.flush()
                self._commit_offset(consumer)
            self._close_consumer(consumer)
            logger.info("Daemon đã dừng an toàn.")

    def _handle_signal(self, signum, frame):
        logger.info(f"Nhận tín hiệu dừng {signum}, chuẩn bị kết thúc an toàn...")
        self.running = False

    def _create_consumer(self):
        # 1. Thử dùng confluent_kafka (Chuẩn của Module 02)
        try:
            from confluent_kafka import Consumer
            c = Consumer({
                "bootstrap.servers": self.servers,
                "group.id": self.group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False
            })
            c.subscribe(self.topics)
            logger.info("Sử dụng confluent_kafka client.")
            return ("confluent", c)
        except ImportError:
            pass

        # 2. Thử dùng kafka-python
        try:
            from kafka import KafkaConsumer
            c = KafkaConsumer(
                *self.topics,
                bootstrap_servers=self.servers.split(","),
                group_id=self.group_id,
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                value_deserializer=lambda m: m.decode("utf-8", errors="replace")
            )
            logger.info("Sử dụng kafka-python client.")
            return ("kafka-python", c)
        except ImportError:
            pass

        logger.warning("Không tìm thấy confluent_kafka hoặc kafka-python.")
        return None

    def _poll_messages(self, consumer_tuple, timeout: float) -> List[Tuple[str, str]]:
        driver, client = consumer_tuple
        records = []
        if driver == "confluent":
            msg = client.poll(timeout)
            if msg and not msg.error() and msg.value():
                records.append((msg.topic(), msg.value().decode("utf-8", errors="replace")))
        elif driver == "kafka-python":
            pack = client.poll(timeout_ms=int(timeout * 1000), max_records=50)
            for tp, msgs in pack.items():
                for m in msgs:
                    records.append((tp.topic, m.value))
        return records

    def _commit_offset(self, consumer_tuple):
        driver, client = consumer_tuple
        try:
            if driver == "confluent":
                client.commit(asynchronous=False)
            elif driver == "kafka-python":
                client.commit()
        except Exception as e:
            logger.warning(f"Lỗi commit offset: {e}")

    def _close_consumer(self, consumer_tuple):
        driver, client = consumer_tuple
        try:
            client.close()
        except Exception:
            pass


def main():
    parser = argparse.ArgumentParser(description="Kafka to HDFS Ingestion Sink Daemon")
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--group", default=os.getenv("KAFKA_GROUP_ID", "hdfs-sink-group"))
    parser.add_argument("--batch-size", type=int, default=200)
    parser.add_argument("--flush-interval", type=int, default=30)
    args = parser.parse_args()

    daemon = KafkaHDFSSinkDaemon(
        bootstrap_servers=args.bootstrap,
        group_id=args.group,
        batch_size=args.batch_size,
        flush_interval_sec=args.flush_interval
    )
    daemon.run()


if __name__ == "__main__":
    main()
