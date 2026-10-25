"""Dựng manifest cho một batch run từ log của Hadoop client (contract mục 9.3: /batch/_meta/run_id=<id>/manifest.json).

Đọc counters của từng job và DỪNG (exit 1) nếu kết quả bất thường, vì Hadoop vẫn báo
"completed successfully" kể cả khi mọi bản ghi bị loại (đã gặp khi chạy thử Job 1).
"""
import argparse
import json
import re
import sys
from pathlib import Path

COLUMNS = {
    "movie_stats": ["movieId", "title", "total_count", "rating_count", "rating_sum", "avg_rating"],
    "movie_daily_stats": ["date", "movieId", "interaction_count", "rating_count", "rating_sum", "avg_rating"],
    "genre_daily_stats": ["date", "genre", "interaction_count", "rating_count", "rating_sum", "avg_rating"],
}
TAIL = ["batch_run_id", "batch_cutoff_time", "generated_at"]
KEEP_FRAMEWORK = ("Map input records", "Map output records", "Reduce input records", "Reduce output records",
                  "Launched map tasks", "Launched reduce tasks", "Failed map tasks", "Failed reduce tasks",
                  "Killed map tasks", "Killed reduce tasks")


def parse_counters(log_text):
    """Phần 'Counters:' của Hadoop client: dòng nhóm thụt 1 tab, dòng counter thụt 2 tab dạng Tên=Số."""
    groups, current = {}, None
    section = log_text.split("Counters:", 1)[-1]
    for line in section.splitlines():
        m = re.match(r"^\t\t([^=\t]+)=(\d+)$", line)
        if m and current is not None:
            groups[current][m.group(1).strip()] = int(m.group(2))
        elif re.match(r"^\t[^\t]", line):
            current = line.strip()
            groups.setdefault(current, {})
    job_id = re.search(r"Running job: (job_\S+)", log_text)
    return groups, job_id.group(1) if job_id else None


def main():
    p = argparse.ArgumentParser()
    for name in ("log-dir", "run-id", "cutoff", "generated-at", "input", "reference", "ref-version", "python-archive"):
        p.add_argument(f"--{name}", required=True)
    p.add_argument("--jobs", nargs="+", required=True)
    a = p.parse_args()

    manifest = {
        "run_id": a.run_id,
        "batch_cutoff_time": a.cutoff,
        "generated_at": a.generated_at,
        "contract_version": "1.0-draft",
        "semantics": "event_time < batch_cutoff_time",
        "input": a.input,
        "reference": {"path": a.reference, "ref_version": a.ref_version},
        "python_archive": a.python_archive,
        "output_format": {"files": "part-NNNNN", "delimiter": "tab", "header": False},
        "jobs": {},
    }
    errors = []
    for job in a.jobs:
        groups, job_id = parse_counters(Path(a.log_dir, f"{job}.log").read_text(encoding="utf-8", errors="replace"))
        framework = {k: v for k, v in groups.get("Map-Reduce Framework", {}).items() if k in KEEP_FRAMEWORK}
        framework.update({k: v for k, v in groups.get("Job Counters", {}).items() if k in KEEP_FRAMEWORK})
        quality = groups.get(job, {})
        map_in, out = framework.get("Map input records", 0), framework.get("Reduce output records", 0)
        invalid = quality.get("invalid_records", 0)
        if map_in == 0 or out == 0 or invalid >= map_in:
            errors.append(f"{job}: map_in={map_in} invalid={invalid} out={out}")
        if invalid:
            print(f"[WARN] {job}: {invalid} bản ghi sai contract bị loại (xem log task)", file=sys.stderr)
        manifest["jobs"][job] = {
            "hadoop_job_id": job_id,
            "output": f"/batch/{job}/run_id={a.run_id}",
            "columns": COLUMNS[job] + TAIL,
            "counters": {"framework": framework, "data_quality": quality},
        }
    if errors:
        sys.exit("[ERROR] Kết quả bất thường, không publish manifest: " + "; ".join(errors))
    json.dump(manifest, sys.stdout, indent=2, ensure_ascii=False)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
