"""Reproducible real-data WSL benchmark using a disposable 512 MB heap broker."""
from __future__ import annotations
import argparse
import json
import os
import platform
import tempfile
import threading
import time
from pathlib import Path
from .common import ROOT, atomic_json, utc_now
from .consumer import consume
from .local_test_broker import TestBroker
from .movie_producer import DEFAULT_RATINGS, replay


def process_sample(pid):
    fields = Path(f"/proc/{pid}/stat").read_text().split(")", 1)[1].split()
    ticks = int(fields[11]) + int(fields[12])
    rss = int(fields[21]) * os.sysconf("SC_PAGE_SIZE") / 1024**2
    return ticks / os.sysconf("SC_CLK_TCK"), rss


def memory():
    values = {}
    for line in Path("/proc/meminfo").read_text().splitlines():
        name, value = line.split(":", 1)
        if name in ("MemTotal", "MemAvailable", "SwapTotal", "SwapFree"):
            values[name + "_MiB"] = int(value.split()[0]) / 1024
    return values


def benchmark(output, counts=(200, 1000, 2000)):
    report = {"measured_at": utc_now(), "platform": platform.platform(), "memory": memory(),
              "broker_heap": "-Xms256m -Xmx512m", "batch_size": 100,
              "input": str(DEFAULT_RATINGS.relative_to(ROOT)), "runs": []}
    with tempfile.TemporaryDirectory(prefix="module02-benchmark-") as tmp:
        directory = Path(tmp)
        broker = TestBroker(directory / "broker")
        try:
            broker.start()
            broker.topics()
            for rate, count in zip((10, 100, 500), counts):
                results, errors = [], []
                def read():
                    try:
                        results.append(consume(broker.bootstrap, "benchmark", directory / "audit.db", limit=count, idle_timeout=30))
                    except Exception as exc:
                        errors.append(str(exc))
                worker = threading.Thread(target=read)
                worker.start()
                pids = {"python": os.getpid(), "kafka": broker.proc.pid}
                baseline = {key: process_sample(pid)[0] for key, pid in pids.items()}
                peaks = {key: 0 for key in pids}
                minimum_available = [memory()["MemAvailable_MiB"]]
                done = threading.Event()
                def sample():
                    while not done.is_set():
                        for key, pid in pids.items():
                            peaks[key] = max(peaks[key], process_sample(pid)[1])
                        minimum_available[0] = min(minimum_available[0], memory()["MemAvailable_MiB"])
                        done.wait(0.2)
                sampler = threading.Thread(target=sample)
                sampler.start()
                start = time.monotonic()
                try:
                    sent = replay(DEFAULT_RATINGS, directory / "checkpoint.json", broker.bootstrap,
                                  rate=rate, batch_size=100, limit=count)
                    worker.join(timeout=60)
                    if worker.is_alive() or errors or not results:
                        raise RuntimeError(f"Benchmark consumer failed: {errors}")
                    received = results[0]
                    if received["valid"] != count or any(received[k] for k in ("invalid", "duplicate", "conflict")):
                        raise RuntimeError(f"Benchmark validation failed: {received}")
                    elapsed = time.monotonic() - start
                    cpu = {key: (process_sample(pid)[0] - baseline[key]) / elapsed * 100 for key, pid in pids.items()}
                finally:
                    done.set()
                    sampler.join()
                run = {"requested_rate": rate, "count": count, "producer": sent, "consumer": received,
                       "peak_rss_MiB": peaks, "cpu_percent_one_core": cpu,
                       "minimum_available_MiB": minimum_available[0]}
                report["runs"].append(run)
                atomic_json(output, report)
                print(json.dumps(run), flush=True)
        finally:
            broker.stop()
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, default=ROOT / "modules/kafka_ingestion/reports/performance.json")
    args = parser.parse_args()
    benchmark(args.report)


if __name__ == "__main__":
    main()
