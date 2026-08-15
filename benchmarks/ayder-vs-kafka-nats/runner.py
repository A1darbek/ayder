#!/usr/bin/env python3
import argparse
import asyncio
import csv
import json
import os
import statistics
import time
from pathlib import Path

import requests


def percentile(values, pct):
    if not values:
        return 0
    ordered = sorted(values)
    idx = int(round((pct / 100.0) * (len(ordered) - 1)))
    return ordered[max(0, min(idx, len(ordered) - 1))]


def payload(i, size):
    prefix = f"{i:012d}:".encode("ascii")
    if len(prefix) >= size:
        return prefix[:size]
    return prefix + (b"x" * (size - len(prefix)))


def wait_until(name, fn, timeout_s=90):
    deadline = time.time() + timeout_s
    last = None
    while time.time() < deadline:
        try:
            if fn():
                return
        except Exception as exc:
            last = exc
        time.sleep(1)
    raise RuntimeError(f"timed out waiting for {name}: {last}")


def summarize(target, lat_us, errors, started, finished, extra=None):
    ok = len(lat_us)
    elapsed = max(finished - started, 1e-9)
    row = {
        "target": target,
        "ok": ok,
        "errors": errors,
        "elapsed_s": round(elapsed, 6),
        "throughput_msg_s": round(ok / elapsed, 2),
        "mean_us": round(statistics.mean(lat_us), 2) if lat_us else 0,
        "p50_us": percentile(lat_us, 50),
        "p95_us": percentile(lat_us, 95),
        "p99_us": percentile(lat_us, 99),
        "max_us": max(lat_us) if lat_us else 0,
    }
    if extra:
        row.update(extra)
    return row


def write_artifacts(artifact_dir, target, summary, lat_us):
    out = Path(artifact_dir)
    out.mkdir(parents=True, exist_ok=True)

    summary_path = out / f"{target}-summary.json"
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    lat_path = out / f"{target}-latencies.csv"
    with lat_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["seq", "latency_us"])
        for i, value in enumerate(lat_us):
            writer.writerow([i, value])

    summary_csv = out / "summary.csv"
    fields = [
        "target",
        "ok",
        "errors",
        "elapsed_s",
        "throughput_msg_s",
        "mean_us",
        "p50_us",
        "p95_us",
        "p99_us",
        "max_us",
    ]
    exists = summary_csv.exists()
    with summary_csv.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        if not exists:
            writer.writeheader()
        writer.writerow(summary)


def bench_ayder(messages, payload_bytes):
    base = os.environ.get("AYDER_BASE", "http://ayder:1109").rstrip("/")
    token = os.environ.get("AYDER_TOKEN", "dev")
    topic = f"bench_ayder_{int(time.time())}"
    headers = {"Authorization": f"Bearer {token}"}

    def ayder_ready():
        response = requests.get(f"{base}/ready", timeout=2)
        return response.status_code == 200 and response.json().get("ready") is True

    wait_until("ayder persistence readiness", ayder_ready)
    r = requests.post(
        f"{base}/broker/topics",
        headers={**headers, "Content-Type": "application/json"},
        data=json.dumps({"name": topic, "partitions": 1}),
        timeout=10,
    )
    r.raise_for_status()
    if not r.json().get("ok"):
        raise RuntimeError(f"topic create failed: {r.text}")

    lat_us = []
    errors = 0
    started = time.perf_counter()
    for i in range(messages):
        body = payload(i, payload_bytes)
        t0 = time.perf_counter_ns()
        try:
            resp = requests.post(
                f"{base}/broker/topics/{topic}/produce?partition=0&timeout_ms=5000",
                headers={**headers, "Content-Type": "application/octet-stream"},
                data=body,
                timeout=10,
            )
            elapsed = (time.perf_counter_ns() - t0) // 1000
            data = resp.json()
            local_fsync_receipt = (
                resp.ok
                and data.get("ok") is True
                and data.get("durability_scope") == "local"
                and data.get("local_fsync") is True
                and data.get("quorum_committed") is False
                and data.get("failover_safe") is False
                and data.get("applied_all_workers") is True
                and data.get("durable") is True
                and data.get("synced") is True
            )
            if local_fsync_receipt:
                lat_us.append(elapsed)
            else:
                errors += 1
        except Exception:
            errors += 1
    finished = time.perf_counter()
    return summarize("ayder", lat_us, errors, started, finished, {"topic": topic}), lat_us


def bench_kafka(messages, payload_bytes):
    from kafka import KafkaAdminClient, KafkaProducer
    from kafka.admin import NewTopic
    from kafka.errors import TopicAlreadyExistsError

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
    topic = f"bench_kafka_{int(time.time())}"

    def can_admin():
        admin = KafkaAdminClient(bootstrap_servers=bootstrap, request_timeout_ms=3000)
        admin.close()
        return True

    wait_until("kafka admin", can_admin, timeout_s=120)
    admin = KafkaAdminClient(bootstrap_servers=bootstrap, request_timeout_ms=10000)
    try:
        admin.create_topics([NewTopic(name=topic, num_partitions=1, replication_factor=1)])
    except TopicAlreadyExistsError:
        pass
    finally:
        admin.close()

    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        acks="all",
        linger_ms=0,
        batch_size=1,
        max_in_flight_requests_per_connection=1,
        retries=0,
        request_timeout_ms=30000,
        value_serializer=lambda v: v,
    )

    lat_us = []
    errors = 0
    started = time.perf_counter()
    for i in range(messages):
        body = payload(i, payload_bytes)
        t0 = time.perf_counter_ns()
        try:
            producer.send(topic, value=body, partition=0).get(timeout=30)
            producer.flush(timeout=30)
            lat_us.append((time.perf_counter_ns() - t0) // 1000)
        except Exception:
            errors += 1
    finished = time.perf_counter()
    producer.close()
    return summarize("kafka", lat_us, errors, started, finished, {"topic": topic}), lat_us


async def bench_nats_async(messages, payload_bytes):
    import nats
    from nats.js.api import RetentionPolicy, StorageType, StreamConfig

    url = os.environ.get("NATS_URL", "nats://nats:4222")
    stream = f"BENCH_{int(time.time())}"
    subject = f"{stream}.events"

    async def can_connect():
        nc = await nats.connect(url, connect_timeout=2)
        await nc.close()
        return True

    deadline = time.time() + 90
    last = None
    while time.time() < deadline:
        try:
            if await can_connect():
                break
        except Exception as exc:
            last = exc
            await asyncio.sleep(1)
    else:
        raise RuntimeError(f"timed out waiting for nats: {last}")

    nc = await nats.connect(url)
    js = nc.jetstream()
    try:
        await js.delete_stream(stream)
    except Exception:
        pass
    await js.add_stream(
        StreamConfig(
            name=stream,
            subjects=[subject],
            storage=StorageType.FILE,
            retention=RetentionPolicy.LIMITS,
            replicas=1,
        )
    )

    lat_us = []
    errors = 0
    started = time.perf_counter()
    for i in range(messages):
        body = payload(i, payload_bytes)
        t0 = time.perf_counter_ns()
        try:
            await js.publish(subject, body, timeout=30)
            lat_us.append((time.perf_counter_ns() - t0) // 1000)
        except Exception:
            errors += 1
    finished = time.perf_counter()
    await nc.close()
    return summarize("nats", lat_us, errors, started, finished, {"stream": stream}), lat_us


def bench_nats(messages, payload_bytes):
    return asyncio.run(bench_nats_async(messages, payload_bytes))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", choices=["ayder", "kafka", "nats"], required=True)
    parser.add_argument("--messages", type=int, default=1000)
    parser.add_argument("--payload-bytes", type=int, default=128)
    parser.add_argument("--artifact-dir", default=os.environ.get("ARTIFACT_DIR", "artifacts/manual"))
    args = parser.parse_args()

    if args.messages < 1:
        raise SystemExit("--messages must be >= 1")
    if args.payload_bytes < 1:
        raise SystemExit("--payload-bytes must be >= 1")

    if args.target == "ayder":
        summary, lat_us = bench_ayder(args.messages, args.payload_bytes)
    elif args.target == "kafka":
        summary, lat_us = bench_kafka(args.messages, args.payload_bytes)
    else:
        summary, lat_us = bench_nats(args.messages, args.payload_bytes)

    write_artifacts(args.artifact_dir, args.target, summary, lat_us)
    print(json.dumps(summary, sort_keys=True))


if __name__ == "__main__":
    main()
