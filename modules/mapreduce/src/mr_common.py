"""Phần dùng chung cho mapper/reducer của Module 04 (Hadoop Streaming, chỉ thư viện chuẩn Python 3).

Được gửi tới các NodeManager qua -files cùng mapper/reducer. -files chỉ tạo symlink trong thư mục làm việc
(file thật nằm ở thư mục cache riêng), nên job chạy với PYTHONPATH=. để `import mr_common` tìm thấy module.
"""
import csv
import json
import math
import os
import re
import sys

UTC_TIME = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
EVENT_ID = re.compile(r"^[0-9a-f]{64}$")
REFERENCE_FILE = "movie_reference.tsv"
UNKNOWN_GENRE = "UNKNOWN"          # cùng quy ước với Module 05 (Thắng) để Module 07 ghép Batch + Realtime đúng
NO_GENRES = "(no genres listed)"


def counter(group, name, amount=1):
    """Tăng một Hadoop counter (giao thức Streaming: ghi ra stderr)."""
    sys.stderr.write(f"reporter:counter:{group},{name},{amount}\n")


def parse_event(line):
    """Trả về (movieId, event_id, rating, event_time) nếu event đúng schema movie-events v1.0, ngược lại None."""
    try:
        e = json.loads(line)
    except ValueError:
        return None
    if not isinstance(e, dict) or e.get("schema_version") != "1.0" or e.get("source") != "movielens":
        return None
    event_id, movie_id, rating, event_time = e.get("event_id"), e.get("movieId"), e.get("rating"), e.get("event_time")
    if not isinstance(event_id, str) or not EVENT_ID.match(event_id):
        return None
    if type(movie_id) is not int or movie_id <= 0:
        return None
    if type(rating) not in (int, float) or not math.isfinite(rating) or not 0.5 <= rating <= 5.0:
        return None
    if not isinstance(event_time, str) or not UTC_TIME.match(event_time):
        return None
    return movie_id, event_id, float(rating), event_time


def read_cutoff():
    """BATCH_CUTOFF_TIME: UTC, tròn giờ (contract mục 5.4)."""
    cutoff = os.environ.get("BATCH_CUTOFF_TIME", "")
    if not UTC_TIME.match(cutoff) or not cutoff.endswith(":00:00Z"):
        sys.exit("BATCH_CUTOFF_TIME phải là UTC tròn giờ, ví dụ 2018-10-01T00:00:00Z")
    return cutoff


def read_tail():
    """Ba cột cuối của mọi Batch View: batch_run_id, batch_cutoff_time, generated_at."""
    tail = [os.environ.get(name, "") for name in ("BATCH_RUN_ID", "BATCH_CUTOFF_TIME", "GENERATED_AT")]
    if not all(tail):
        sys.exit("Thiếu BATCH_RUN_ID / BATCH_CUTOFF_TIME / GENERATED_AT")
    return tail


def load_reference(path=REFERENCE_FILE):
    """movieId -> (title, genres). Đọc bằng csv, không tự cắt theo tab (contract mục 8.1)."""
    if not os.path.isfile(path):
        return None
    with open(path, encoding="utf-8", newline="") as f:
        return {int(r["movieId"]): (r["title"], r["genres"]) for r in csv.DictReader(f, delimiter="\t")}


def run_mapper(group, key_fn):
    """Vòng lặp mapper chung: kiểm tra event, lọc event_time < T, in `key TAB event_id TAB rating` cho mỗi key."""
    cutoff = read_cutoff()
    logged = 0
    for line in sys.stdin:
        if not line.strip():
            continue
        event = parse_event(line)
        if event is None:
            counter(group, "invalid_records")           # sai contract: bỏ nhưng có đếm
            if logged < 3:                               # ghi vài dòng mẫu vào log task để truy nguyên nhân
                sys.stderr.write(f"invalid_record_sample: {line.strip()[:300]}\n")
                logged += 1
            continue
        movie_id, event_id, rating, event_time = event
        if event_time >= cutoff:                        # chuỗi UTC cùng định dạng so sánh được theo thời gian
            counter(group, "after_cutoff_skipped")      # thuộc Realtime View (Module 05)
            continue
        for key in key_fn(movie_id, event_time):
            print(f"{key}\t{event_id}\t{rating}")


def run_reducer(group, emit_fn):
    """Vòng lặp reducer chung. Input đã được Hadoop sắp xếp theo key nên các dòng cùng key nằm liền nhau.
    Với mỗi key: loại event trùng theo event_id (contract 16.2) rồi gọi emit_fn(key, count, rating_sum)."""
    current, ratings = None, {}
    for line in sys.stdin:
        if not line.strip():
            continue
        key, event_id, rating = line.rstrip("\n").split("\t")
        if key != current:                              # sang key mới -> in kết quả key cũ
            if current is not None:
                emit_fn(current, len(ratings), math.fsum(ratings.values()))
            current, ratings = key, {}
        if event_id in ratings:                         # event trùng (sink ghi 2 lần) -> chỉ tính 1 lần
            counter(group, "duplicates_removed")
            continue
        ratings[event_id] = float(rating)
    if current is not None:
        emit_fn(current, len(ratings), math.fsum(ratings.values()))


def stats(count, rating_sum):
    """interaction_count/total_count, rating_count, rating_sum, avg_rating (MovieLens: 1 tương tác = 1 rating)."""
    return [str(count), str(count), str(rating_sum), str(rating_sum / count)]
