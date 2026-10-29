#!/usr/bin/env python3
"""
test_kafka_hdfs_sink.py - Bộ kiểm thử tích hợp Kafka-to-HDFS Sink
Kiểm tra toàn diện:
1. Quy tắc trích xuất phân vùng ngày dt=YYYY-MM-DD từ event_time
2. Kiểm tra tính hợp lệ của schema (movie-events và tmdb-snapshots)
3. Cơ chế cách ly bản ghi lỗi vào /quarantine/ (Quarantine Isolation)
4. Cơ chế gom batch và flush an toàn vào HDFS theo chuẩn JSON Lines
5. Cam kết At-least-once và làm sạch buffer sau khi flush thành công
"""

import hashlib
import json
import unittest
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

# Nạp script kafka_hdfs_sink
import sys
MODULE_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(MODULE_DIR / "scripts"))

from kafka_hdfs_sink import (
    HDFSSinkEngine,
    KafkaHDFSSinkDaemon,
    extract_partition_date,
    validate_schema,
)


def make_dummy_event_id(seed: str) -> str:
    """Tạo event_id chuẩn SHA-256 64 hex chars."""
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()


class MockHDFSUploader:
    """Mock HDFS Uploader để kiểm tra đường dẫn và payload ghi xuống HDFS."""

    def __init__(self, succeed: bool = True, fail_after: int = 0):
        self.succeed = succeed
        self.fail_after = fail_after
        self.write_calls = 0
        self.written_files: Dict[str, str] = {}
        self.deleted_files: List[str] = []

    def write_file(self, hdfs_path: str, data: str) -> bool:
        self.write_calls += 1
        if self.fail_after > 0 and self.write_calls > self.fail_after:
            return False
        if self.succeed:
            self.written_files[hdfs_path] = data
            return True
        return False

    def delete_file(self, hdfs_path: str) -> bool:
        self.deleted_files.append(hdfs_path)
        self.written_files.pop(hdfs_path, None)
        return True


class TestKafkaHDFSSink(unittest.TestCase):

    def setUp(self):
        self.uploader = MockHDFSUploader(succeed=True)
        self.engine = HDFSSinkEngine(uploader=self.uploader, batch_size=10, flush_interval_sec=5)

    def test_01_extract_partition_date_from_event_time(self):
        """[Test 1] Trích xuất ngày dt=YYYY-MM-DD chính xác từ event_time UTC."""
        dt1 = extract_partition_date("2026-10-04T05:30:00Z")
        self.assertEqual(dt1, "2026-10-04")

        dt2 = extract_partition_date("2026-12-31T23:59:59+00:00")
        self.assertEqual(dt2, "2026-12-31")

        # Fallback an toàn khi chuỗi ngày hỏng
        dt_bad = extract_partition_date("invalid-date-string")
        self.assertEqual(dt_bad, "1970-01-01")

    def test_02_validate_movie_events_schema(self):
        """[Test 2] Kiểm tra hợp đồng dữ liệu cho topic movie-events."""
        valid_event = {
            "schema_version": "1.0",
            "event_id": make_dummy_event_id("movie-101-1-1728043200"),
            "source": "movielens",
            "userId": 101,
            "movieId": 1,
            "rating": 4.5,
            "event_time": "2026-10-04T12:00:00Z",
            "ingest_time": "2026-10-11T12:00:00Z"
        }
        is_valid, msg = validate_schema(valid_event, "movie-events")
        self.assertTrue(is_valid, f"Bản ghi hợp lệ nhưng bị báo lỗi: {msg}")

        # Trường hợp thiếu trường bắt buộc
        invalid_missing = valid_event.copy()
        del invalid_missing["userId"]
        is_valid, msg = validate_schema(invalid_missing, "movie-events")
        self.assertFalse(is_valid)

        # Trường hợp sai kiểu dữ liệu rating
        invalid_type = valid_event.copy()
        invalid_type["rating"] = "invalid_float"
        is_valid, msg = validate_schema(invalid_type, "movie-events")
        self.assertFalse(is_valid)

        # Trường hợp event_id sai format (không phải SHA-256 64 chars)
        invalid_id = valid_event.copy()
        invalid_id["event_id"] = "bad-uuid"
        is_valid, msg = validate_schema(invalid_id, "movie-events")
        self.assertFalse(is_valid)

    def test_03_validate_tmdb_snapshots_schema(self):
        """[Test 3] Kiểm tra hợp đồng dữ liệu cho topic tmdb-snapshots."""
        valid_snapshot = {
            "schema_version": "1.0",
            "event_id": make_dummy_event_id("tmdb-550-20261004"),
            "source": "tmdb",
            "tmdbId": 550,
            "title": "Fight Club",
            "popularity": 85.5,
            "vote_average": 8.4,
            "vote_count": 25000,
            "event_time": "2026-10-04T00:00:00Z",
            "ingest_time": "2026-10-11T00:00:00Z"
        }
        is_valid, msg = validate_schema(valid_snapshot, "tmdb-snapshots")
        self.assertTrue(is_valid, f"TMDB snapshot hợp lệ nhưng bị báo lỗi: {msg}")

        # Sai nguồn source
        invalid_source = valid_snapshot.copy()
        invalid_source["source"] = "wrong_source"
        is_valid, msg = validate_schema(invalid_source, "tmdb-snapshots")
        self.assertFalse(is_valid)

    def test_04_process_valid_records_routing(self):
        """[Test 4] Điều hướng bản ghi hợp lệ vào đúng phân vùng ngày trong raw_buffers."""
        event_1 = json.dumps({
            "schema_version": "1.0",
            "event_id": make_dummy_event_id("ev-01"),
            "source": "movielens",
            "userId": 1,
            "movieId": 100,
            "rating": 5.0,
            "event_time": "2026-10-04T08:00:00Z",
            "ingest_time": "2026-10-11T08:00:00Z"
        })
        event_2 = json.dumps({
            "schema_version": "1.0",
            "event_id": make_dummy_event_id("ev-02"),
            "source": "movielens",
            "userId": 2,
            "movieId": 200,
            "rating": 3.0,
            "event_time": "2026-10-05T09:00:00Z",
            "ingest_time": "2026-10-11T09:00:00Z"
        })

        self.engine.process_record("movie-events", event_1)
        self.engine.process_record("movie-events", event_2)

        self.assertEqual(len(self.engine.raw_buffers[("ratings", "2026-10-04")]), 1)
        self.assertEqual(len(self.engine.raw_buffers[("ratings", "2026-10-05")]), 1)
        self.assertEqual(self.engine.total_valid, 2)
        self.assertEqual(self.engine.total_quarantined, 0)

    def test_05_quarantine_isolation_on_bad_records(self):
        """[Test 5] Cô lập hoàn toàn bản ghi lỗi vào quarantine mà không nhiễm bẩn vùng raw."""
        # 1. JSON hỏng cú pháp
        bad_json = "{bad_json_string"
        ok1, _ = self.engine.process_record("movie-events", bad_json)
        self.assertFalse(ok1)

        # 2. Thiếu trường bắt buộc
        missing_event = json.dumps({"schema_version": "1.0", "rating": 4.0})
        ok2, _ = self.engine.process_record("movie-events", missing_event)
        self.assertFalse(ok2)

        # 3. Bản ghi hợp lệ chen giữa
        good_event = json.dumps({
            "schema_version": "1.0",
            "event_id": make_dummy_event_id("ev-good"),
            "source": "movielens",
            "userId": 5,
            "movieId": 50,
            "rating": 4.0,
            "event_time": "2026-10-04T10:00:00Z",
            "ingest_time": "2026-10-11T10:00:00Z"
        })
        ok3, _ = self.engine.process_record("movie-events", good_event)
        self.assertTrue(ok3)

        self.assertEqual(self.engine.total_valid, 1)
        self.assertEqual(self.engine.total_quarantined, 2)
        self.assertEqual(len(self.engine.raw_buffers[("ratings", "2026-10-04")]), 1)

    def test_06_flush_creates_valid_hdfs_json_lines(self):
        """[Test 6] Flush ghi đúng cấu trúc đường dẫn HDFS và định dạng JSON Lines."""
        record = json.dumps({
            "schema_version": "1.0",
            "event_id": make_dummy_event_id("ev-flush-01"),
            "source": "movielens",
            "userId": 10,
            "movieId": 100,
            "rating": 4.0,
            "event_time": "2026-10-04T10:00:00Z",
            "ingest_time": "2026-10-11T10:00:00Z"
        })
        self.engine.process_record("movie-events", record)
        self.assertEqual(self.engine.pending_count(), 1)

        stats = self.engine.flush()
        self.assertEqual(stats["raw_written"], 1)
        self.assertEqual(self.engine.pending_count(), 0)

        # Kiểm tra file đã ghi
        self.assertEqual(len(self.uploader.written_files), 1)
        path = list(self.uploader.written_files.keys())[0]
        self.assertTrue(path.startswith("/raw/ratings/dt=2026-10-04/part-"))
        self.assertTrue(path.endswith(".jsonl"))

        # Kiểm tra nội dung là JSON hợp lệ
        content = self.uploader.written_files[path].strip()
        parsed = json.loads(content)
        self.assertEqual(parsed["userId"], 10)

    def test_07_quarantine_flush_includes_error_metadata(self):
        """[Test 7] Bản ghi quarantine flush ra file chứa metadata lỗi và thời gian."""
        bad_record = json.dumps({"schema_version": "1.0", "broken": True})
        self.engine.process_record("movie-events", bad_record)

        stats = self.engine.flush()
        self.assertEqual(stats["quarantine_written"], 1)
        self.assertEqual(self.engine.pending_count(), 0)

        path = list(self.uploader.written_files.keys())[0]
        self.assertTrue(path.startswith("/quarantine/movielens/dt="))
        content = self.uploader.written_files[path].strip()
        parsed = json.loads(content)
        self.assertIn("error_reason", parsed)
        self.assertIn("error_time", parsed)

    def test_08_at_least_once_and_buffer_retention_on_failure(self):
        """[Test 8] Nếu upload thất bại, giữ nguyên buffer để retry (bảo toàn dữ liệu)."""
        failing_uploader = MockHDFSUploader(succeed=False)
        failing_engine = HDFSSinkEngine(uploader=failing_uploader, batch_size=10)

        record = json.dumps({
            "schema_version": "1.0",
            "event_id": make_dummy_event_id("ev-retry-01"),
            "source": "movielens",
            "userId": 1,
            "movieId": 1,
            "rating": 5.0,
            "event_time": "2026-10-04T12:00:00Z",
            "ingest_time": "2026-10-11T12:00:00Z"
        })
        failing_engine.process_record("movie-events", record)
        self.assertEqual(failing_engine.pending_count(), 1)

        stats = failing_engine.flush()
        self.assertEqual(stats["failed"], 1)
        # Buffer không bị xóa khi ghi thất bại!
        self.assertEqual(failing_engine.pending_count(), 1)

    def test_09_partial_flush_failure_rolls_back_written_files(self):
        """[Test 9] Cách 1: Nếu flush lỗi giữa chừng (ghi được 1 ngày, ngày thứ 2 lỗi), tự động xóa file ngày 1 trên HDFS và giữ nguyên buffer."""
        partial_uploader = MockHDFSUploader(succeed=True, fail_after=1)
        engine = HDFSSinkEngine(uploader=partial_uploader, batch_size=10)

        ev_day1 = json.dumps({
            "schema_version": "1.0",
            "event_id": make_dummy_event_id("ev-day1"),
            "source": "movielens",
            "userId": 1,
            "movieId": 10,
            "rating": 4.0,
            "event_time": "2026-10-04T12:00:00Z",
            "ingest_time": "2026-10-11T12:00:00Z"
        })
        ev_day2 = json.dumps({
            "schema_version": "1.0",
            "event_id": make_dummy_event_id("ev-day2"),
            "source": "movielens",
            "userId": 2,
            "movieId": 20,
            "rating": 5.0,
            "event_time": "2026-10-05T12:00:00Z",
            "ingest_time": "2026-10-11T12:00:00Z"
        })
        engine.process_record("movie-events", ev_day1)
        engine.process_record("movie-events", ev_day2)
        self.assertEqual(engine.pending_count(), 2)

        stats = engine.flush(atomic_rollback=True)
        # File thứ nhất đã bị xóa rollback, không còn file lẻ trên HDFS
        self.assertEqual(stats["rolled_back_files"], 1)
        self.assertEqual(stats["raw_written"], 0)
        self.assertEqual(stats["failed"], 2)
        self.assertEqual(len(partial_uploader.written_files), 0)
        self.assertEqual(len(partial_uploader.deleted_files), 1)
        # Cả 2 bản ghi đều được khôi phục lại trong buffer
        self.assertEqual(engine.pending_count(), 2)

    def test_10_rebalance_commit_failure_recovers_offset_without_duplication(self):
        """[Test 10] Cách 2: Khi flush thành công nhưng commit lỗi do max.poll.interval.ms, giữ nguyên HDFS và bỏ qua đoạn đã ghi khi poll lại."""
        daemon = KafkaHDFSSinkDaemon(batch_size=2, flush_interval_sec=5)
        daemon.engine.uploader = self.uploader

        class FakeMsg:
            def __init__(self, topic: str, partition: int, offset: int, val: str):
                self._topic = topic
                self._partition = partition
                self._offset = offset
                self._val = val.encode("utf-8")

            def error(self):
                return None

            def topic(self):
                return self._topic

            def partition(self):
                return self._partition

            def offset(self):
                return self._offset

            def value(self):
                return self._val

        class FakeConfluentClient:
            def __init__(self):
                self.batches = []
                self.fail_next_commit = True
                self.seeked = []
                self.commits = 0

            def consume(self, num_messages=10, timeout=1.0):
                return self.batches.pop(0) if self.batches else []

            def commit(self, *args, **kwargs):
                if self.fail_next_commit:
                    self.fail_next_commit = False
                    raise RuntimeError("KafkaError{code=_MAX_POLL_EXCEEDED}")
                self.commits += 1

            def seek(self, tp):
                self.seeked.append((tp.topic, tp.partition, tp.offset))

        ev0 = json.dumps({
            "schema_version": "1.0",
            "event_id": make_dummy_event_id("ev-off-0"),
            "source": "movielens",
            "userId": 1,
            "movieId": 1,
            "rating": 4.0,
            "event_time": "2026-10-04T12:00:00Z",
            "ingest_time": "2026-10-11T12:00:00Z"
        })
        ev1 = json.dumps({
            "schema_version": "1.0",
            "event_id": make_dummy_event_id("ev-off-1"),
            "source": "movielens",
            "userId": 2,
            "movieId": 2,
            "rating": 5.0,
            "event_time": "2026-10-04T12:01:00Z",
            "ingest_time": "2026-10-11T12:01:00Z"
        })
        ev2 = json.dumps({
            "schema_version": "1.0",
            "event_id": make_dummy_event_id("ev-off-2"),
            "source": "movielens",
            "userId": 3,
            "movieId": 3,
            "rating": 3.5,
            "event_time": "2026-10-04T12:02:00Z",
            "ingest_time": "2026-10-11T12:02:00Z"
        })

        client = FakeConfluentClient()
        # Lô 1: offset 0 và 1. Lô 2 (sau khi bị Rebalance): Kafka trả lại từ offset 0, 1 kèm bản ghi mới offset 2
        client.batches = [
            [FakeMsg("movie-events", 0, 0, ev0), FakeMsg("movie-events", 0, 1, ev1)],
            [FakeMsg("movie-events", 0, 0, ev0), FakeMsg("movie-events", 0, 1, ev1), FakeMsg("movie-events", 0, 2, ev2)],
        ]
        consumer_tuple = ("confluent", client)

        # Đợt 1: Poll offset 0, 1 -> Flush thành công -> Commit bị lỗi MAX_POLL_EXCEEDED
        msgs1 = daemon._poll_messages(consumer_tuple, 1.0)
        self.assertEqual(len(msgs1), 2)
        for t, v in msgs1:
            daemon.engine.process_record(t, v)
        daemon._execute_flush_and_commit(consumer_tuple)

        # Kiểm tra: Đã ghi HDFS 1 file (2 dòng), flushed_offsets lưu mốc 2, bật cờ needs_offset_recovery
        self.assertEqual(daemon.flushed_offsets[("movie-events", 0)], 2)
        self.assertTrue(daemon.needs_offset_recovery)
        self.assertEqual(len(self.uploader.written_files), 1)

        # Đợt 2: Sau Rebalance, Kafka trả lại offset 0, 1, 2 -> Sink tự bỏ qua 0, 1 và chỉ nhận offset 2!
        msgs2 = daemon._poll_messages(consumer_tuple, 1.0)
        self.assertEqual(len(msgs2), 1)
        for t, v in msgs2:
            daemon.engine.process_record(t, v)
        daemon._execute_flush_and_commit(consumer_tuple)

        # Tổng số bản ghi hợp lệ đã xử lý đúng bằng 3 (không bị nhân đôi offset 0 và 1!)
        self.assertEqual(daemon.engine.total_valid, 3)
        self.assertFalse(daemon.needs_offset_recovery)


if __name__ == "__main__":
    unittest.main()
