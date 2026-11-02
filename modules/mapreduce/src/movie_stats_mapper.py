#!/usr/bin/env python3
"""Job 1 — movie_stats — MAPPER. Key = movieId (contract mục 10.1).

In: ratings thô JSON Lines (schema movie-events v1.0). Out: `movieId TAB event_id TAB rating`.
Env: BATCH_CUTOFF_TIME (UTC tròn giờ). Chỉ giữ event có event_time < BATCH_CUTOFF_TIME.
"""
from mr_common import run_mapper

if __name__ == "__main__":
    run_mapper("movie_stats", lambda movie_id, event_time: [str(movie_id)])
