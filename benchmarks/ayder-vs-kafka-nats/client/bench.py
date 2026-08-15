#!/usr/bin/env python3
import argparse
import asyncio
import csv
import json
import os
import socket
import statistics
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import aiohttp
import nats
from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from nats.js.api import StorageType, StreamConfig
from nats.js.errors import NotFoundError


def now_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def payload_for(seq, payload_bytes):
    prefix = f"{seq:020d}|".encode()
    if payload_bytes <= len(prefix):
        return prefix[:payload_bytes]
    return prefix + (b"x" * (payload_bytes - len(prefix)))


def percentile(sorted_values, pct):
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return sorted_values[0]
    rank = (pct / 100.0) * (len(sorted_values) - 1)
    low = int(rank)
    high = min(low + 1, len(sorted_values) - 1)
    frac = rank - low
    return sorted_values[low] + (sorted_values[high] - sorted_values[low]) * frac


def summarize(service, args, samples, started, finished):
    latencies = sorted(s["latency_ms"] for s in samples if s["ok"])
    errors = [s for s in samples if not s["ok"]]
    duration_s = max(finished - started, 0.000001)
    success = len(latencies)
    return {
        "ts": now_iso(),
        "run_id": args.run_id,
        "service": service,
        "messages": args.messages,
        "success": success,
        "errors": len(errors),
        "concurrency": args.concurrency,
        "payload_bytes": args.payload_bytes,
        "duration_s": round(duration_s, 6),
        "throughput_msg_s": round(success / duration_s, 3),
        "latency_avg_ms": round(statistics.fmean(latencies), 3) if latencies else 0.0,
        "latency_min_ms": round(latencies[0], 3) if latencies else 0.0,
        "latency_p50_ms": round(percentile(latencies, 50), 3),
        "latency_p95_ms": round(percentile(latencies, 95), 3),
        "latency_p99_ms": round(percentile(latencies, 99), 3),
        "latency_max_ms": round(latencies[-1], 3) if latencies else 0.0,
    }


async def wait_tcp(host, port, deadline):
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=2):
                return True
        except OSError:
            await asyncio.sleep(1)
    return False


async def wait_ayder(args, deadline):
    url = f"{args.ayder_url.rstrip('/')}/ready"
    async with aiohttp.ClientSession() as session:
        while time.monotonic() < deadline:
            try:
                async with session.get(url, timeout=3) as resp:
                    body = await resp.json(content_type=None)
                    if resp.status == 200 and body.get("ready") is True:
                        return
            except Exception:
                pass
            await asyncio.sleep(1)
    raise RuntimeError(f"Ayder did not become ready at {url}")


async def wait_nats(args, deadline):
    while time.monotonic() < deadline:
        try:
            nc = await nats.connect(args.nats_url, connect_timeout=2)
            await nc.close()
            return
        except Exception:
            await asyncio.sleep(1)
    raise RuntimeError(f"NATS did not become ready at {args.nats_url}")


async def wait_kafka(args, deadline):
    admin = AdminClient({"bootstrap.servers": args.kafka_bootstrap})
    while time.monotonic() < deadline:
        try:
            admin.list_topics(timeout=3)
            return
        except Exception:
            await asyncio.sleep(1)
    raise RuntimeError(f"Kafka did not become ready at {args.kafka_bootstrap}")


async def wait_all(args):
    deadline = time.monotonic() + args.wait_seconds
    await asyncio.gather(
        wait_ayder(args, deadline),
        wait_kafka(args, deadline),
        wait_nats(args, deadline),
    )
    print(json.dumps({"ts": now_iso(), "event": "ready"}), flush=True)


async def setup_ayder(args, topic):
    headers = {
        "Authorization": f"Bearer {args.ayder_token}",
        "Content-Type": "application/json",
    }
    url = f"{args.ayder_url.rstrip('/')}/broker/topics"
    body = {"name": topic, "partitions": 1}
    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, json=body, timeout=10) as resp:
            text = await resp.text()
            if resp.status >= 400:
                raise RuntimeError(f"Ayder topic create failed: HTTP {resp.status}: {text}")


async def bench_ayder(args):
    topic = f"{args.topic_prefix}-{args.run_id}-ayder"
    await setup_ayder(args, topic)
    counter = 0
    lock = asyncio.Lock()
    samples = []
    headers = {
        "Authorization": f"Bearer {args.ayder_token}",
        "Content-Type": "application/octet-stream",
    }
    url = f"{args.ayder_url.rstrip('/')}/broker/topics/{topic}/produce"
    params = {
        "partition": "0",
        "timeout_ms": str(args.timeout_ms),
        "timing": "1",
        "durable": "1",
    }
    timeout = aiohttp.ClientTimeout(total=max(5, args.timeout_ms / 1000 + 5))

    async def next_seq():
        nonlocal counter
        async with lock:
            if counter >= args.messages:
                return None
            seq = counter
            counter += 1
            return seq

    async def worker():
        async with aiohttp.ClientSession(timeout=timeout) as session:
            while True:
                seq = await next_seq()
                if seq is None:
                    return
                started_ns = time.perf_counter_ns()
                ok = False
                error = ""
                try:
                    async with session.post(url, params=params, headers=headers,
                                            data=payload_for(seq, args.payload_bytes)) as resp:
                        text = await resp.text()
                        ok = resp.status < 400
                        if ok:
                            try:
                                body = json.loads(text)
                                ok = (
                                    body.get("ok") is True
                                    and body.get("durability_scope") == "local"
                                    and body.get("local_fsync") is True
                                    and body.get("quorum_committed") is False
                                    and body.get("failover_safe") is False
                                    and body.get("applied_all_workers") is True
                                    and body.get("durable") is True
                                    and body.get("synced") is True
                                )
                                if not ok:
                                    error = f"invalid_local_fsync_receipt:{text[:240]}"
                            except json.JSONDecodeError:
                                error = f"bad_json:{text[:160]}"
                                ok = False
                        else:
                            error = f"http_{resp.status}:{text[:160]}"
                except Exception as exc:
                    error = repr(exc)
                latency_ms = (time.perf_counter_ns() - started_ns) / 1_000_000
                samples.append({"service": "ayder", "seq": seq, "latency_ms": latency_ms,
                                "ok": ok, "error": error})

    started = time.perf_counter()
    await asyncio.gather(*(worker() for _ in range(args.concurrency)))
    finished = time.perf_counter()
    return summarize("ayder", args, samples, started, finished), samples


def ensure_kafka_topic(args, topic):
    admin = AdminClient({"bootstrap.servers": args.kafka_bootstrap})
    new_topic = NewTopic(topic, num_partitions=1, replication_factor=1)
    futures = admin.create_topics([new_topic], request_timeout=30)
    try:
        futures[topic].result(timeout=30)
    except KafkaException as exc:
        if "TOPIC_ALREADY_EXISTS" not in str(exc):
            raise
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        metadata = admin.list_topics(topic=topic, timeout=5)
        if topic in metadata.topics and not metadata.topics[topic].error:
            return
        time.sleep(1)
    raise RuntimeError(f"Kafka topic {topic} was not visible after create")


def kafka_worker(args, topic, worker_id):
    producer = Producer({
        "bootstrap.servers": args.kafka_bootstrap,
        "acks": "all",
        "linger.ms": 0,
        "batch.num.messages": 1,
        "queue.buffering.max.messages": 1,
        "message.timeout.ms": max(args.timeout_ms, 5000),
        "socket.timeout.ms": max(args.timeout_ms, 5000),
        "enable.idempotence": False,
    })
    samples = []
    for seq in range(worker_id, args.messages, args.concurrency):
        done = threading.Event()
        result = {"ok": False, "error": ""}

        def on_delivery(err, msg):
            if err is None:
                result["ok"] = True
            else:
                result["error"] = str(err)
            done.set()

        started_ns = time.perf_counter_ns()
        try:
            producer.produce(topic, value=payload_for(seq, args.payload_bytes),
                             key=str(seq).encode(), on_delivery=on_delivery)
            while not done.wait(0.001):
                producer.poll(0)
        except Exception as exc:
            result["error"] = repr(exc)
        finally:
            producer.poll(0)
        latency_ms = (time.perf_counter_ns() - started_ns) / 1_000_000
        samples.append({"service": "kafka", "seq": seq, "latency_ms": latency_ms,
                        "ok": result["ok"], "error": result["error"]})
    producer.flush(10)
    return samples


async def bench_kafka(args):
    topic = f"{args.topic_prefix}-{args.run_id}-kafka"
    ensure_kafka_topic(args, topic)
    started = time.perf_counter()
    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        chunks = await asyncio.gather(*(
            loop.run_in_executor(pool, kafka_worker, args, topic, worker_id)
            for worker_id in range(args.concurrency)
        ))
    finished = time.perf_counter()
    samples = [sample for chunk in chunks for sample in chunk]
    return summarize("kafka", args, samples, started, finished), samples


async def setup_nats(args, stream, subject):
    nc = await nats.connect(args.nats_url)
    js = nc.jetstream()
    try:
        await js.delete_stream(stream)
    except NotFoundError:
        pass
    await js.add_stream(StreamConfig(name=stream, subjects=[subject], storage=StorageType.FILE))
    return nc, js


async def bench_nats(args):
    stream = f"BENCH_{args.run_id.replace('-', '_').upper()}_NATS"
    subject = f"{args.topic_prefix}.{args.run_id}.nats"
    nc, js = await setup_nats(args, stream, subject)
    counter = 0
    lock = asyncio.Lock()
    samples = []

    async def next_seq():
        nonlocal counter
        async with lock:
            if counter >= args.messages:
                return None
            seq = counter
            counter += 1
            return seq

    async def worker():
        while True:
            seq = await next_seq()
            if seq is None:
                return
            started_ns = time.perf_counter_ns()
            ok = False
            error = ""
            try:
                ack = await js.publish(subject, payload_for(seq, args.payload_bytes),
                                       timeout=max(1, args.timeout_ms / 1000))
                ok = ack.stream == stream
                if not ok:
                    error = f"unexpected_ack_stream:{ack.stream}"
            except Exception as exc:
                error = repr(exc)
            latency_ms = (time.perf_counter_ns() - started_ns) / 1_000_000
            samples.append({"service": "nats", "seq": seq, "latency_ms": latency_ms,
                            "ok": ok, "error": error})

    started = time.perf_counter()
    await asyncio.gather(*(worker() for _ in range(args.concurrency)))
    finished = time.perf_counter()
    await nc.drain()
    return summarize("nats", args, samples, started, finished), samples


def write_outputs(args, summaries, all_samples):
    out_dir = Path(args.artifacts_dir) / args.run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    metadata = {
        "ts": now_iso(),
        "run_id": args.run_id,
        "host": socket.gethostname(),
        "messages": args.messages,
        "concurrency": args.concurrency,
        "payload_bytes": args.payload_bytes,
        "timeout_ms": args.timeout_ms,
        "services": args.services,
        "durability": {
            "ayder": "HTTP produce, --aof always, timeout_ms waits for synced=true",
            "kafka": "acks=all client, batch.num.messages=1, linger.ms=0, broker log.flush.interval.messages=1",
            "nats": "JetStream file stream, awaited publish ack per message",
        },
    }
    (out_dir / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")

    with (out_dir / "results.jsonl").open("a", encoding="utf-8") as fh:
        for summary in summaries:
            fh.write(json.dumps(summary, sort_keys=True) + "\n")

    fieldnames = list(summaries[0].keys()) if summaries else []
    with (out_dir / "summary.csv").open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(summaries)

    with (out_dir / "samples.csv").open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=["service", "seq", "latency_ms", "ok", "error"])
        writer.writeheader()
        for sample in sorted(all_samples, key=lambda row: (row["service"], row["seq"])):
            row = dict(sample)
            row["latency_ms"] = round(row["latency_ms"], 6)
            writer.writerow(row)

    print(json.dumps({"ts": now_iso(), "event": "artifacts", "path": str(out_dir)}), flush=True)


async def run(args):
    summaries = []
    all_samples = []
    services = [svc.strip() for svc in args.services.split(",") if svc.strip()]
    runners = {
        "ayder": bench_ayder,
        "kafka": bench_kafka,
        "nats": bench_nats,
    }
    for service in services:
        if service not in runners:
            raise RuntimeError(f"Unknown service {service!r}; expected ayder,kafka,nats")
        print(json.dumps({"ts": now_iso(), "event": "start", "service": service}), flush=True)
        summary, samples = await runners[service](args)
        summaries.append(summary)
        all_samples.extend(samples)
        print(json.dumps({"ts": now_iso(), "event": "summary", **summary}), flush=True)
    write_outputs(args, summaries, all_samples)


def parser():
    p = argparse.ArgumentParser(description="Durable produce benchmark for Ayder, Kafka, and NATS JetStream")
    sub = p.add_subparsers(dest="cmd", required=True)

    def common(sp):
        sp.add_argument("--ayder-url", default=os.getenv("AYDER_URL", "http://ayder:1109"))
        sp.add_argument("--ayder-token", default=os.getenv("AYDER_TOKEN", "dev"))
        sp.add_argument("--kafka-bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"))
        sp.add_argument("--nats-url", default=os.getenv("NATS_URL", "nats://nats:4222"))

    wait_p = sub.add_parser("wait")
    common(wait_p)
    wait_p.add_argument("--wait-seconds", type=int, default=int(os.getenv("WAIT_SECONDS", "180")))

    run_p = sub.add_parser("run")
    common(run_p)
    run_p.add_argument("--services", default=os.getenv("SERVICES", "ayder,kafka,nats"))
    run_p.add_argument("--messages", type=int, default=int(os.getenv("MESSAGES", "1000")))
    run_p.add_argument("--concurrency", type=int, default=int(os.getenv("CONCURRENCY", "1")))
    run_p.add_argument("--payload-bytes", type=int, default=int(os.getenv("PAYLOAD_BYTES", "256")))
    run_p.add_argument("--timeout-ms", type=int, default=int(os.getenv("TIMEOUT_MS", "5000")))
    run_p.add_argument("--topic-prefix", default=os.getenv("TOPIC_PREFIX", "bench"))
    run_p.add_argument("--run-id", default=os.getenv("RUN_ID", time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())))
    run_p.add_argument("--artifacts-dir", default=os.getenv("ARTIFACTS_DIR", "/artifacts"))
    return p


def main():
    args = parser().parse_args()
    if getattr(args, "concurrency", 1) < 1:
        raise SystemExit("--concurrency must be >= 1")
    if getattr(args, "messages", 1) < 1:
        raise SystemExit("--messages must be >= 1")
    if getattr(args, "payload_bytes", 1) < 1:
        raise SystemExit("--payload-bytes must be >= 1")
    if args.cmd == "wait":
        asyncio.run(wait_all(args))
    elif args.cmd == "run":
        asyncio.run(run(args))


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(json.dumps({"ts": now_iso(), "event": "error", "error": str(exc)}), file=sys.stderr, flush=True)
        raise
