#!/usr/bin/env python3
"""Job 2 — movie_daily_stats — REDUCER (contract mục 10.2).

Out: date, movieId, interaction_count, rating_count, rating_sum, avg_rating, batch_run_id, batch_cutoff_time, generated_at
"""
from mr_common import read_tail, run_reducer, stats

if __name__ == "__main__":
    tail = read_tail()

    def emit(key, count, rating_sum):
        date, movie_id = key.split("|", 1)
        print("\t".join([date, movie_id] + stats(count, rating_sum) + tail))

    run_reducer("movie_daily_stats", emit)
