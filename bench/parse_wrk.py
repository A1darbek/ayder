#!/usr/bin/env python3
import csv
import json
import re
import statistics
import sys
from pathlib import Path


VALIDATION_RE = re.compile(r"([a-z0-9_]+)=([0-9]+)")
LATENCY_RE = re.compile(r"(p50|p95|p99|p99_9|max)=([0-9]+(?:\.[0-9]+)?)")
RPS_RE = re.compile(r"^Requests/sec:\s+([0-9]+(?:\.[0-9]+)?)", re.MULTILINE)


def parse_trial(path: Path) -> dict:
    text = path.read_text(encoding="utf-8", errors="replace")
    validation_line = next(
        (line for line in text.splitlines() if line.startswith("AYDER_VALIDATION ")), None
    )
    latency_line = next(
        (line for line in text.splitlines() if line.startswith("AYDER_LATENCY_US ")), None
    )
    rps_match = RPS_RE.search(text)
    if validation_line is None or latency_line is None or rps_match is None:
        raise ValueError(f"{path}: incomplete wrk2 output")

    validation = {key: int(value) for key, value in VALIDATION_RE.findall(validation_line)}
    latency_us = {key: float(value) for key, value in LATENCY_RE.findall(latency_line)}
    required_validation = {
        "requests", "ok", "invalid", "status_2xx", "status_4xx", "status_429",
        "status_5xx", "connect_errors", "read_errors", "write_errors", "timeout_errors",
    }
    required_latency = {"p50", "p95", "p99", "p99_9", "max"}
    if not required_validation.issubset(validation):
        raise ValueError(f"{path}: missing validation counters")
    if not required_latency.issubset(latency_us):
        raise ValueError(f"{path}: missing latency percentiles")

    clean = (
        validation["requests"] == validation["ok"]
        and validation["invalid"] == 0
        and validation["status_4xx"] == 0
        and validation["status_5xx"] == 0
        and validation["connect_errors"] == 0
        and validation["read_errors"] == 0
        and validation["write_errors"] == 0
        and validation["timeout_errors"] == 0
    )
    result = {
        "trial": int(path.stem.split("-")[-1]),
        "achieved_rps": float(rps_match.group(1)),
        "p50_ms": latency_us["p50"] / 1000.0,
        "p95_ms": latency_us["p95"] / 1000.0,
        "p99_ms": latency_us["p99"] / 1000.0,
        "p99_9_ms": latency_us["p99_9"] / 1000.0,
        "max_ms": latency_us["max"] / 1000.0,
        **validation,
        "validation_clean": clean,
    }
    sample_path = path.with_name(path.name.replace(".txt", ".system.tsv"))
    if sample_path.exists():
        with sample_path.open(newline="", encoding="utf-8") as handle:
            samples = list(csv.DictReader(handle, delimiter="\t"))
        if samples:
            numeric = [{key: int(value) for key, value in sample.items()} for sample in samples]
            result.update(
                system_samples=len(numeric),
                server_cpu_ticks_delta=max(0, numeric[-1]["cpu_ticks"] - numeric[0]["cpu_ticks"]),
                server_rss_kb_max=max(row["rss_kb"] for row in numeric),
                server_read_bytes_delta=max(0, numeric[-1]["read_bytes"] - numeric[0]["read_bytes"]),
                server_write_bytes_delta=max(0, numeric[-1]["write_bytes"] - numeric[0]["write_bytes"]),
                server_threads_max=max(row["threads"] for row in numeric),
            )
    return result


def summarize(rows: list[dict]) -> dict:
    metrics = ["achieved_rps", "p50_ms", "p95_ms", "p99_ms", "p99_9_ms", "max_ms"]
    optional_metrics = [
        "server_cpu_ticks_delta", "server_rss_kb_max", "server_read_bytes_delta",
        "server_write_bytes_delta", "server_threads_max",
    ]
    metrics.extend(metric for metric in optional_metrics if all(metric in row for row in rows))
    summary = {"trial_count": len(rows), "all_trials_clean": all(r["validation_clean"] for r in rows)}
    for metric in metrics:
        values = [float(row[metric]) for row in rows]
        summary[f"{metric}_median"] = statistics.median(values)
        summary[f"{metric}_min"] = min(values)
        summary[f"{metric}_max"] = max(values)
        summary[f"{metric}_pstdev"] = statistics.pstdev(values)
    return summary


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: parse_wrk.py OUTPUT_DIR", file=sys.stderr)
        return 2
    output_dir = Path(sys.argv[1])
    paths = sorted(output_dir.glob("trial-*.txt"))
    if not paths:
        print(f"no trial files under {output_dir}", file=sys.stderr)
        return 2
    try:
        rows = [parse_trial(path) for path in paths]
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    summary = summarize(rows)
    (output_dir / "trials.json").write_text(json.dumps(rows, indent=2) + "\n", encoding="utf-8")
    (output_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    with (output_dir / "trials.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    print(json.dumps(summary, sort_keys=True))
    return 0 if summary["all_trials_clean"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
