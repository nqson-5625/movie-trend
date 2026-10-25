#!/usr/bin/env python3
"""Job 3 — genre_daily_stats — MAPPER. Key = `date|genre` (contract mục 10.3).

Mỗi event được "explode" thành một dòng cho mỗi thể loại của phim (lấy từ movie_reference.tsv qua -files).
Phát (1 event, rating) chứ không chỉ 1, để reducer tính được trung bình (contract 10.3).
Thể loại: movieId không có trong reference, genres rỗng hoặc "(no genres listed)" -> UNKNOWN,
giống Module 05 để Module 07 ghép Batch + Realtime đúng nhóm. Join miss được đếm (contract 17.2).
"""
import sys

from mr_common import NO_GENRES, UNKNOWN_GENRE, counter, load_reference, run_mapper

if __name__ == "__main__":
    reference = load_reference()
    if reference is None:
        sys.exit("Thiếu movie_reference.tsv (gửi bằng -files ...#movie_reference.tsv)")

    def keys(movie_id, event_time):
        date = event_time[:10]
        row = reference.get(movie_id)
        if row is None:
            counter("genre_daily_stats", "genre_join_miss")
            return [f"{date}|{UNKNOWN_GENRE}"]
        genres = [g for g in row[1].split("|") if g and g != NO_GENRES]
        if not genres:
            counter("genre_daily_stats", "unknown_genre_events")
            return [f"{date}|{UNKNOWN_GENRE}"]
        return [f"{date}|{g}" for g in genres]

    run_mapper("genre_daily_stats", keys)
