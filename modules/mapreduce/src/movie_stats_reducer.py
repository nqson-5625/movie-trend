#!/usr/bin/env python3
"""Job 1 — movie_stats — REDUCER (contract mục 10.1).

Out: movieId, title, total_count, rating_count, rating_sum, avg_rating, batch_run_id, batch_cutoff_time, generated_at
Env: BATCH_RUN_ID, BATCH_CUTOFF_TIME, GENERATED_AT. Title lấy từ movie_reference.tsv (gửi qua -files), không bắt buộc.
"""
from mr_common import counter, load_reference, read_tail, run_reducer, stats

if __name__ == "__main__":
    tail = read_tail()
    reference = load_reference() or {}

    def emit(movie_id, count, rating_sum):
        title = reference.get(int(movie_id), ("", ""))[0]
        if not title:
            counter("movie_stats", "title_missing")
        print("\t".join([movie_id, title] + stats(count, rating_sum) + tail))

    run_reducer("movie_stats", emit)
