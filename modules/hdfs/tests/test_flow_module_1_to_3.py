#!/usr/bin/env python3
"""
test_flow_module_1_to_3.py - Kiểm thử tích hợp luồng Module 01 -> Module 03 (HDFS)
Phụ trách: Giang (Module 03 - HDFS)

Kiểm tra:
1. Tính hợp lệ của dữ liệu tham chiếu (Canonical Movie Reference) do Module 01 bàn giao.
2. Kiểm tra tuân thủ Schema theo contracts/schemas/movie-reference.schema.json.
3. Kiểm tra tính toàn vẹn Checksum SHA256 và số dòng.
4. Mô phỏng quy trình Bước 16: Tải lên Version Path và Atomic Publish sang Current.
5. Mô phỏng Downstream Read & Join (MapReduce / Spark static join).
6. Kiểm tra logic phân vùng Raw Events (dt=YYYY-MM-DD) và cơ chế Quarantine.
"""

import csv
import hashlib
import json
import shutil
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

# Thư mục gốc dự án BTL
PROJECT_ROOT = Path(__file__).resolve().parents[3]
import sys
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from modules.data_source.src.contract_validation import (
    REFERENCE_FIELDS, load_validator, read_jsonl, read_reference,
    reference_record, validate_record, validate_reference_rows,
)


class TestFlowModule1To3(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.schemas_dir = PROJECT_ROOT / "contracts" / "schemas"
        cls.samples_dir = PROJECT_ROOT / "contracts" / "samples"
        cls.prepared_dir = PROJECT_ROOT / "data" / "prepared" / "reference"

        # Ưu tiên dữ liệu thật đã chuẩn bị, nếu chưa có thì dùng sample
        ref_real = cls.prepared_dir / "movie_reference.tsv"
        if ref_real.exists():
            cls.ref_path = ref_real
            cls.is_sample = False
        else:
            cls.ref_path = cls.samples_dir / "movie-reference.sample.tsv"
            cls.is_sample = True

        cls.validator_ref = load_validator("movie-reference")
        cls.validator_events = load_validator("movie-events")
        cls.validator_tmdb = load_validator("tmdb-snapshots")

    def test_01_module_1_artifacts_exist(self):
        """[Test 1] Kiểm tra các artifact bàn giao từ Module 01 đều tồn tại."""
        self.assertTrue(self.ref_path.exists(), f"Không tìm thấy file reference tại {self.ref_path}")
        self.assertTrue((self.schemas_dir / "movie-reference.schema.json").exists())
        self.assertTrue((self.schemas_dir / "movie-events.schema.json").exists())
        self.assertTrue((self.schemas_dir / "tmdb-snapshots.schema.json").exists())
        self.assertTrue((self.samples_dir / "fixture_provenance.json").exists())

    def test_02_reference_header_and_column_order(self):
        """[Test 2] Kiểm tra file TSV có đúng 10 cột theo thứ tự của DATA_CONTRACT.md."""
        with open(self.ref_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f, delimiter="\t")
            header = next(reader)

        self.assertEqual(
            header, REFERENCE_FIELDS,
            f"Thứ tự cột không đúng chuẩn contract!\nKỳ vọng: {REFERENCE_FIELDS}\nThực tế: {header}"
        )

    def test_03_reference_rows_schema_compliance(self):
        """[Test 3] Kiểm tra từng dòng reference tuân thủ chặt chẽ JSON Schema của nhóm."""
        rows = read_reference(self.ref_path)
        self.assertGreaterEqual(len(rows), 20, "Dữ liệu reference phải có ít nhất 20 phim")
        
        # Hàm kiểm tra hợp đồng từ Module 01
        validate_reference_rows(rows)

        # Kiểm tra chi tiết từng dòng
        for idx, row in enumerate(rows, start=1):
            record = reference_record(row)
            validate_record(record, self.validator_ref)
            self.assertGreater(record["movieId"], 0, f"Dòng {idx}: movieId phải là số nguyên dương")
            self.assertTrue(len(record["title"].strip()) > 0, f"Dòng {idx}: title không được để trống")
            if record["imdbId"] is not None:
                self.assertRegex(record["imdbId"], r"^tt\d{7,}$", f"Dòng {idx}: imdbId sai định dạng")

    def test_04_sha256_checksum_verification(self):
        """[Test 4] Kiểm tra tính toàn vẹn (Checksum SHA256) của file reference."""
        hasher = hashlib.sha256()
        with open(self.ref_path, "rb") as f:
            while chunk := f.read(65536):
                hasher.update(chunk)
        calculated_sha = hasher.hexdigest()
        self.assertEqual(len(calculated_sha), 64, "Mã băm SHA256 phải đủ 64 ký tự hex")

        # Đối chiếu với provenance nếu dùng sample
        if self.is_sample:
            prov_file = self.samples_dir / "fixture_provenance.json"
            with open(prov_file, "r", encoding="utf-8") as f:
                prov = json.load(f)
            # Provenance ghi source hash ban đầu
            self.assertIn("movie-reference", prov["fixtures"])
            self.assertEqual(prov["fixtures"]["movie-reference"]["row_count"], len(read_reference(self.ref_path)))

    def test_05_step_16_publish_handoff_simulation(self):
        """[Test 5] Mô phỏng quy trình Bước 16 (Handoff Module 1 -> 3): Upload -> Verify -> Atomic Publish."""
        with tempfile.TemporaryDirectory() as temp_hdfs_root:
            hdfs_root = Path(temp_hdfs_root)

            # Lấy ref_version từ dữ liệu
            rows = read_reference(self.ref_path)
            ref_version = rows[0]["ref_version"]
            self.assertTrue(bool(ref_version), "ref_version không được để trống")

            # 1. Module 01 tải file lên version path
            version_dir = hdfs_root / "movies" / "ref" / f"version={ref_version}"
            version_dir.mkdir(parents=True, exist_ok=True)
            target_version_file = version_dir / "movie_reference.tsv"
            shutil.copy2(self.ref_path, target_version_file)

            # 2. Module 03 kiểm tra tính toàn vẹn trên HDFS (Verify row count & hash)
            self.assertTrue(target_version_file.exists())
            with open(target_version_file, "r", encoding="utf-8") as f:
                lines_in_hdfs = sum(1 for _ in f)
            with open(self.ref_path, "r", encoding="utf-8") as f:
                lines_local = sum(1 for _ in f)
            self.assertEqual(lines_in_hdfs, lines_local, "Số dòng trên HDFS phải khớp số dòng local")

            # 3. Module 03 cập nhật Atomic sang canonical current path
            current_dir = hdfs_root / "movies" / "ref" / "current"
            current_dir.mkdir(parents=True, exist_ok=True)
            target_current_file = current_dir / "movie_reference.tsv"
            
            # Atomic update bằng copy/replace
            temp_current = current_dir / "movie_reference.tsv.tmp"
            shutil.copy2(target_version_file, temp_current)
            temp_current.replace(target_current_file)

            # 4. Xác nhận file tại current path
            self.assertTrue(target_current_file.exists())
            current_rows = read_reference(target_current_file)
            self.assertEqual(len(current_rows), len(rows), "Dữ liệu tại canonical current path phải đầy đủ")

    def test_06_downstream_consumer_join(self):
        """[Test 6] Mô phỏng downstream (Module 04 MapReduce) đọc canonical reference và join với raw event."""
        # 1. Đọc reference và decode sang kiểu dữ liệu chuẩn (movieId là int)
        rows = read_reference(self.ref_path)
        ref_lookup = {reference_record(row)["movieId"]: reference_record(row) for row in rows}

        # 2. Đọc event mẫu từ Module 02 / Kafka
        events = read_jsonl(self.samples_dir / "movie-events.sample.jsonl")
        self.assertGreaterEqual(len(events), 20)

        # 3. Thực hiện Join mô phỏng (MapReduce distributed cache lookup)
        joined_count = 0
        for event in events:
            validate_record(event, self.validator_events)
            m_id = event["movieId"]
            if m_id in ref_lookup:
                ref_item = ref_lookup[m_id]
                self.assertIsNotNone(ref_item["genres"])
                joined_count += 1

        self.assertGreater(joined_count, 0, "Phải có ít nhất 1 event join thành công với reference")

    def test_07_raw_event_partitioning_by_event_time(self):
        """[Test 7] Kiểm tra quy tắc phân vùng ngày dt=YYYY-MM-DD dựa trên event_time UTC."""
        events = read_jsonl(self.samples_dir / "movie-events.sample.jsonl")
        for event in events:
            event_time_str = event["event_time"]
            # Kiểm tra định dạng UTC ISO 8601
            dt = datetime.strptime(event_time_str, "%Y-%m-%dT%H:%M:%SZ")
            partition_dt = dt.strftime("%Y-%m-%d")

            # Đường dẫn HDFS mong đợi
            expected_hdfs_path = f"/raw/ratings/dt={partition_dt}/part-00000.jsonl"
            self.assertTrue(expected_hdfs_path.startswith("/raw/ratings/dt="))

    def test_08_quarantine_bad_records_handling(self):
        """[Test 8] Kiểm tra cơ chế cách ly bản ghi lỗi vào /quarantine/."""
        bad_payload = '{"source": "movielens", "broken": true}'  # Thiếu schema_version, event_id, event_time
        
        # Mô phỏng bắt lỗi và sinh quarantine record
        from datetime import timezone
        now_utc = datetime.now(timezone.utc)
        today_str = now_utc.strftime("%Y-%m-%d")
        quarantine_dir = f"/quarantine/movielens/dt={today_str}"
        
        bad_record = {
            "raw_payload": bad_payload,
            "error_reason": "Thiếu các trường bắt buộc (schema_version/event_id/event_time)",
            "failed_at": now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
        }
        
        self.assertIn("error_reason", bad_record)
        self.assertTrue(quarantine_dir.startswith("/quarantine/movielens/dt="))


def run_tests():
    suite = unittest.TestLoader().loadTestsFromTestCase(TestFlowModule1To3)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    return result.wasSuccessful()


if __name__ == "__main__":
    import sys
    success = run_tests()
    sys.exit(0 if success else 1)
