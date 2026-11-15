#!/usr/bin/env python3
"""Job 2 — movie_daily_stats — MAPPER. Key = `date|movieId`, date = ngày UTC của event_time (contract mục 10.2).

Out: `YYYY-MM-DD|movieId TAB event_id TAB rating`. Env: BATCH_CUTOFF_TIME.
"""
from mr_common import run_mapper

if __name__ == "__main__":
    run_mapper("movie_daily_stats", lambda movie_id, event_time: [f"{event_time[:10]}|{movie_id}"])
