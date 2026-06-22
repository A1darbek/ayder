#!/usr/bin/env python3
import json
import os
import random
import signal
import sqlite3
import subprocess
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


def now_utc():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def log(msg):
    print(f"[{now_utc()}] {msg}", flush=True)


def getenv_int(name, default):
    return int(os.getenv(name, str(default)))


class AyderManager:
    def __init__(self, binary, port, workers, data_dir, log_file):
        self.binary = binary
        self.port = port
        self.workers = workers
        self.data_dir = data_dir
        self.log_file = log_file
        self.proc = None
        self.stop = False
        self.restarts = 0
        self.lock = threading.Lock()
        self.thread = threading.Thread(target=self._loop, daemon=True)

    def start(self):
        self.thread.start()

    def _loop(self):
        env = os.environ.copy()
        env.setdefault(
            "RF_BEARER_TOKENS",
            "dev@55555555555555:11111111111111111:111111111111111111111",
        )
        env.setdefault("RF_HTTP_DISABLE_RL", "1")
        while not self.stop:
            Path(self.data_dir).mkdir(parents=True, exist_ok=True)
            with open(self.log_file, "a", encoding="utf-8") as lf:
                proc = subprocess.Popen(
                    [self.binary, "--port", str(self.port), "--workers", str(self.workers)],
                    cwd=self.data_dir,
                    stdout=lf,
                    stderr=lf,
                    env=env,
                )
            with self.lock:
                self.proc = proc
            rc = proc.wait()
            with self.lock:
                self.proc = None
            if self.stop:
                break
            self.restarts += 1
            log(f"ayder exited rc={rc}; restarting")
            time.sleep(0.6)

    def kill_once(self):
        with self.lock:
            p = self.proc
        if p and p.poll() is None:
            p.kill()
            return True
        return False

    def shutdown(self):
        self.stop = True
        with self.lock:
            p = self.proc
        if p and p.poll() is None:
            p.terminate()
            try:
                p.wait(timeout=3)
            except subprocess.TimeoutExpired:
                p.kill()
        self.thread.join(timeout=4)


class SinkState:
    def __init__(self, db_path, fail_pct):
        self.db_path = db_path
        self.fail_pct = fail_pct
        self.lock = threading.Lock()
        self._init_db()

    def _conn(self):
        return sqlite3.connect(self.db_path, timeout=30)

    def _init_db(self):
        conn = self._conn()
        conn.execute(
            "CREATE TABLE IF NOT EXISTS deliveries (event_id TEXT PRIMARY KEY, first_seen_utc TEXT NOT NULL, attempts INT NOT NULL DEFAULT 0)"
        )
        conn.commit()
        conn.close()

    def accept(self, event_id):
        with self.lock:
            if random.randint(1, 100) <= self.fail_pct:
                return False, False
            conn = self._conn()
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM deliveries WHERE event_id=?", (event_id,))
            existed = cur.fetchone() is not None
            if existed:
                cur.execute(
                    "UPDATE deliveries SET attempts=attempts+1 WHERE event_id=?",
                    (event_id,),
                )
            else:
                cur.execute(
                    "INSERT INTO deliveries(event_id, first_seen_utc, attempts) VALUES (?, ?, 1)",
                    (event_id, now_utc()),
                )
            conn.commit()
            conn.close()
            return True, existed


class SinkHandler(BaseHTTPRequestHandler):
    state = None

    def do_POST(self):
        if self.path != "/webhook":
            self.send_response(404)
            self.end_headers()
            return
        n = int(self.headers.get("Content-Length", "0"))
        payload = self.rfile.read(n) if n > 0 else b"{}"
        try:
            obj = json.loads(payload.decode("utf-8"))
            event_id = obj.get("event_id", "")
        except Exception:
            event_id = ""
        if not event_id:
            self.send_response(400)
            self.end_headers()
            return
        ok, existed = self.state.accept(event_id)
        if not ok:
            self.send_response(500)
            self.end_headers()
            self.wfile.write(b'{"ok":false,"error":"injected_failure"}')
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(
            json.dumps({"ok": True, "duplicate": bool(existed)}).encode("utf-8")
        )

    def log_message(self, fmt, *args):
        return


class Campaign:
    def __init__(self):
        self.token = os.getenv("TOKEN", "dev")
        self.total_events = getenv_int("TOTAL_EVENTS", 200)
        self.dup_every = getenv_int("DUP_EVERY", 10)
        self.max_retries = getenv_int("MAX_RETRIES", 8)
        self.retry_sleep_ms = getenv_int("RETRY_SLEEP_MS", 120)
        self.consume_timeout = float(os.getenv("CONSUME_TIMEOUT_SEC", "1.2"))
        self.ayder_port = getenv_int("AYDER_PORT", 1159)
        self.ayder_base = os.getenv("AYDER_BASE", f"http://127.0.0.1:{self.ayder_port}")
        self.ayder_bin = os.getenv("AYDER_BIN", str(Path.cwd() / "ayder"))
        self.ayder_workers = getenv_int("AYDER_WORKERS", 2)
        self.topic = os.getenv("TOPIC", "e2e_webhook_outbox")
        self.group = os.getenv("GROUP", "e2e_g1")
        self.partition = getenv_int("PARTITION", 0)
        self.sink_port = getenv_int("SINK_PORT", 18080)
        self.sink_fail_pct = getenv_int("SINK_FAIL_PCT", 8)
        self.crash_consume_pct = getenv_int("FAULT_CRASH_AFTER_CONSUME_PCT", 8)
        self.crash_db_pct = getenv_int("FAULT_CRASH_BEFORE_DB_COMMIT_PCT", 8)
        self.crash_dispatch_before_pct = getenv_int("FAULT_CRASH_DISPATCH_BEFORE_SEND_PCT", 6)
        self.crash_dispatch_after_pct = getenv_int("FAULT_CRASH_DISPATCH_AFTER_SEND_PCT", 6)
        self.enable_partition = getenv_int("ENABLE_PARTITION", 1) == 1
        self.partition_every_sec = getenv_int("PARTITION_EVERY_SEC", 25)
        self.partition_hold_sec = getenv_int("PARTITION_HOLD_SEC", 5)
        self.enable_ayder_kill = getenv_int("ENABLE_AYDER_KILL", 1) == 1
        self.ayder_kill_every_sec = getenv_int("AYDER_KILL_EVERY_SEC", 45)
        self.duration_sec = getenv_int("DURATION_SEC", 180)

        run_id = f"webhook_outbox_{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}"
        self.artifact_dir = Path(
            os.getenv("ARTIFACT_DIR", str(Path.cwd() / "artifacts" / "e2e_patterns" / run_id))
        )
        self.artifact_dir.mkdir(parents=True, exist_ok=True)
        self.db = str(self.artifact_dir / "pattern.db")
        self.sink_db = str(self.artifact_dir / "sink.db")
        self.ayder_log = str(self.artifact_dir / "ayder.log")
        self.summary_file = self.artifact_dir / "summary.json"
        self.invariants_file = self.artifact_dir / "invariants.json"

        self.partition_until = 0.0
        self.stop_flag = False
        self.metrics = {
            "consumer_crash_injections": 0,
            "dispatcher_crash_injections": 0,
            "partition_injections": 0,
            "ayder_sigkill_injections": 0,
        }

    def _conn(self):
        return sqlite3.connect(self.db, timeout=30, isolation_level=None)

    def init_db(self):
        conn = self._conn()
        cur = conn.cursor()
        cur.executescript(
            """
            CREATE TABLE IF NOT EXISTS expected_events (
              event_id TEXT PRIMARY KEY,
              account_id TEXT NOT NULL,
              delta INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS business_effects (
              event_id TEXT PRIMARY KEY,
              msg_offset INTEGER NOT NULL,
              account_id TEXT NOT NULL,
              delta INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS account_balances (
              account_id TEXT PRIMARY KEY,
              balance INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS consumer_state (
              topic TEXT NOT NULL,
              grp TEXT NOT NULL,
              partition_id INTEGER NOT NULL,
              last_offset INTEGER NOT NULL,
              PRIMARY KEY (topic, grp, partition_id)
            );
            CREATE TABLE IF NOT EXISTS state_history (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              prev_offset INTEGER NOT NULL,
              new_offset INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS outbox (
              event_id TEXT PRIMARY KEY,
              payload_json TEXT NOT NULL,
              status TEXT NOT NULL
            );
            """
        )
        conn.close()

    def json_request(self, method, url, payload=None, timeout=2.0, headers=None):
        if self.enable_partition and "127.0.0.1" in url and time.time() < self.partition_until:
            raise urllib.error.URLError("injected_partition")
        data = None
        req_headers = {"Authorization": f"Bearer {self.token}"}
        if headers:
            req_headers.update(headers)
        if payload is not None:
            data = json.dumps(payload).encode("utf-8")
            req_headers["Content-Type"] = "application/json"
        req = urllib.request.Request(url, data=data, method=method, headers=req_headers)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = r.read().decode("utf-8")
            if not body:
                return {}
            return json.loads(body)

    def ensure_topic(self):
        try:
            self.json_request(
                "POST",
                f"{self.ayder_base}/broker/topics",
                {"name": self.topic, "partitions": 1},
                timeout=1.5,
            )
        except Exception:
            pass

    def wait_ayder(self, sec=60):
        until = time.time() + sec
        while time.time() < until:
            try:
                obj = self.json_request("GET", f"{self.ayder_base}/health", timeout=0.6)
                if obj.get("ok") == 1:
                    return True
            except Exception:
                pass
            time.sleep(0.25)
        return False

    def produce(self):
        conn = self._conn()
        cur = conn.cursor()
        for i in range(1, self.total_events + 1):
            event_id = f"evt-{i:06d}"
            account_id = f"acct-{(i % 7) + 1:02d}"
            payload = {"event_id": event_id, "account_id": account_id, "delta": 1}
            cur.execute(
                "INSERT OR IGNORE INTO expected_events(event_id, account_id, delta) VALUES (?, ?, 1)",
                (event_id, account_id),
            )
            ok = False
            for _ in range(self.max_retries):
                try:
                    resp = self.json_request(
                        "POST",
                        f"{self.ayder_base}/broker/topics/{self.topic}/produce?partition={self.partition}&timeout_ms=5000&idempotency_key={urllib.parse.quote(event_id)}",
                        payload,
                        timeout=1.5,
                    )
                    if resp.get("ok") is True:
                        ok = True
                        break
                except Exception:
                    pass
                time.sleep(self.retry_sleep_ms / 1000.0)
            if not ok:
                raise RuntimeError(f"producer failed for {event_id}")
            if self.dup_every > 0 and i % self.dup_every == 0:
                try:
                    self.json_request(
                        "POST",
                        f"{self.ayder_base}/broker/topics/{self.topic}/produce?partition={self.partition}&timeout_ms=5000&idempotency_key={urllib.parse.quote(event_id)}",
                        payload,
                        timeout=1.5,
                    )
                except Exception:
                    pass
        conn.commit()
        conn.close()

    def consumer_step(self):
        conn = self._conn()
        cur = conn.cursor()
        cur.execute(
            "SELECT COALESCE((SELECT last_offset FROM consumer_state WHERE topic=? AND grp=? AND partition_id=?), -1)",
            (self.topic, self.group, self.partition),
        )
        next_off = cur.fetchone()[0] + 1
        conn.close()

        try:
            resp = self.json_request(
                "GET",
                f"{self.ayder_base}/broker/consume/{self.topic}/{self.group}/{self.partition}?offset={next_off}&limit=1&encoding=b64",
                timeout=self.consume_timeout,
            )
        except Exception:
            return "transient"
        if int(resp.get("count", 0)) == 0:
            return "idle"

        msg = resp["messages"][0]
        event = json.loads(
            subprocess.check_output(
                ["bash", "-lc", f"printf %s {msg['value_b64']} | base64 -d"], text=True
            )
        )
        event_id = event["event_id"]
        account_id = event["account_id"]
        delta = int(event["delta"])
        msg_offset = int(msg["offset"])

        if random.randint(1, 100) <= self.crash_consume_pct:
            self.metrics["consumer_crash_injections"] += 1
            return "crash"
        if random.randint(1, 100) <= self.crash_db_pct:
            self.metrics["consumer_crash_injections"] += 1
            return "crash"

        conn = self._conn()
        cur = conn.cursor()
        cur.execute("BEGIN")
        cur.execute("SELECT COALESCE((SELECT last_offset FROM consumer_state WHERE topic=? AND grp=? AND partition_id=?), -1)", (self.topic, self.group, self.partition))
        prev = int(cur.fetchone()[0])
        new_off = msg_offset if msg_offset > prev else prev
        cur.execute(
            "INSERT OR IGNORE INTO business_effects(event_id, msg_offset, account_id, delta) VALUES (?, ?, ?, ?)",
            (event_id, msg_offset, account_id, delta),
        )
        inserted = cur.rowcount > 0
        if inserted:
            cur.execute(
                "INSERT OR IGNORE INTO outbox(event_id, payload_json, status) VALUES (?, ?, 'pending')",
                (event_id, json.dumps(event)),
            )
            cur.execute(
                "INSERT INTO account_balances(account_id, balance) VALUES (?, ?) ON CONFLICT(account_id) DO UPDATE SET balance=balance+excluded.balance",
                (account_id, delta),
            )
        cur.execute(
            "INSERT INTO consumer_state(topic, grp, partition_id, last_offset) VALUES (?, ?, ?, ?) ON CONFLICT(topic, grp, partition_id) DO UPDATE SET last_offset=max(last_offset, excluded.last_offset)",
            (self.topic, self.group, self.partition, new_off),
        )
        cur.execute(
            "INSERT INTO state_history(prev_offset, new_offset) VALUES (?, ?)",
            (prev, new_off),
        )
        cur.execute("COMMIT")
        conn.close()

        commit_payload = {
            "topic": self.topic,
            "group": self.group,
            "partition": self.partition,
            "offset": msg_offset,
        }
        for _ in range(5):
            try:
                c = self.json_request(
                    "POST",
                    f"{self.ayder_base}/broker/commit",
                    commit_payload,
                    timeout=self.consume_timeout,
                )
                if c.get("ok") is True:
                    break
            except Exception:
                pass
            time.sleep(0.08)
        return "ok"

    def dispatcher_step(self):
        conn = self._conn()
        cur = conn.cursor()
        cur.execute("SELECT event_id, payload_json FROM outbox WHERE status='pending' ORDER BY rowid LIMIT 1")
        row = cur.fetchone()
        conn.close()
        if not row:
            return "idle"
        event_id, payload_json = row
        payload = json.loads(payload_json)

        if random.randint(1, 100) <= self.crash_dispatch_before_pct:
            self.metrics["dispatcher_crash_injections"] += 1
            return "crash"

        try:
            self.json_request(
                "POST",
                f"http://127.0.0.1:{self.sink_port}/webhook",
                payload,
                timeout=1.2,
                headers={"Idempotency-Key": event_id},
            )
        except Exception:
            return "transient"

        if random.randint(1, 100) <= self.crash_dispatch_after_pct:
            self.metrics["dispatcher_crash_injections"] += 1
            return "crash"

        conn = self._conn()
        cur = conn.cursor()
        cur.execute("UPDATE outbox SET status='sent' WHERE event_id=?", (event_id,))
        conn.commit()
        conn.close()
        return "ok"

    def count_row(self, sql):
        conn = self._conn()
        cur = conn.cursor()
        cur.execute(sql)
        v = int(cur.fetchone()[0])
        conn.close()
        return v

    def run_fault_injector(self, manager: AyderManager):
        start = time.time()
        next_part = self.partition_every_sec
        next_kill = self.ayder_kill_every_sec
        while not self.stop_flag and time.time() - start < self.duration_sec:
            elapsed = int(time.time() - start)
            if self.enable_partition and elapsed >= next_part:
                self.partition_until = time.time() + self.partition_hold_sec
                self.metrics["partition_injections"] += 1
                log("fault: injected logical partition")
                next_part += self.partition_every_sec
            if self.enable_ayder_kill and elapsed >= next_kill:
                if manager.kill_once():
                    self.metrics["ayder_sigkill_injections"] += 1
                    log("fault: injected ayder SIGKILL")
                next_kill += self.ayder_kill_every_sec
            time.sleep(0.25)

    def run(self):
        log(f"campaign_start artifact_dir={self.artifact_dir}")
        self.init_db()

        sink_state = SinkState(self.sink_db, self.sink_fail_pct)
        SinkHandler.state = sink_state
        sink_srv = ThreadingHTTPServer(("127.0.0.1", self.sink_port), SinkHandler)
        sink_thread = threading.Thread(target=sink_srv.serve_forever, daemon=True)
        sink_thread.start()

        manager = AyderManager(
            self.ayder_bin,
            self.ayder_port,
            self.ayder_workers,
            str(self.artifact_dir / "ayder_data"),
            self.ayder_log,
        )
        manager.start()
        if not self.wait_ayder(90):
            raise RuntimeError("ayder failed to become healthy")
        self.ensure_topic()

        fi_thread = threading.Thread(target=self.run_fault_injector, args=(manager,), daemon=True)
        fi_thread.start()

        self.produce()
        log("producer_done")

        idle_c = 0
        while self.count_row("SELECT COUNT(*) FROM business_effects") < self.total_events:
            r = self.consumer_step()
            if r == "idle":
                idle_c += 1
                time.sleep(0.08)
            elif r in ("transient", "crash"):
                time.sleep(0.08)
            else:
                idle_c = 0
            if idle_c > 800:
                raise RuntimeError("consumer idle timeout")
        log("consumer_done")

        idle_d = 0
        while True:
            sent = self.count_row("SELECT COUNT(*) FROM outbox WHERE status='sent'")
            pending = self.count_row("SELECT COUNT(*) FROM outbox WHERE status='pending'")
            if sent >= self.total_events and pending == 0:
                break
            r = self.dispatcher_step()
            if r == "idle":
                idle_d += 1
                time.sleep(0.08)
            elif r in ("transient", "crash"):
                time.sleep(0.08)
            else:
                idle_d = 0
            if idle_d > 1200:
                raise RuntimeError("dispatcher idle timeout")
        log("dispatcher_done")

        self.stop_flag = True
        fi_thread.join(timeout=2)
        sink_srv.shutdown()
        manager.shutdown()

        self.verify_and_write()
        log("campaign_success")

    def verify_and_write(self):
        expected = self.count_row("SELECT COUNT(*) FROM expected_events")
        business = self.count_row("SELECT COUNT(*) FROM business_effects")
        pending = self.count_row("SELECT COUNT(*) FROM outbox WHERE status='pending'")
        violations = self.count_row("SELECT COUNT(*) FROM state_history WHERE new_offset < prev_offset")
        sum_expected = self.count_row("SELECT COALESCE(SUM(delta),0) FROM expected_events")
        sum_bal = self.count_row("SELECT COALESCE(SUM(balance),0) FROM account_balances")
        conn = sqlite3.connect(self.sink_db, timeout=30)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM deliveries")
        sink_unique = int(cur.fetchone()[0])
        conn.close()

        inv = {
            "expected_events": expected,
            "business_effects": business,
            "sink_unique_deliveries": sink_unique,
            "pending_outbox": pending,
            "monotonic_violations": violations,
            "sum_expected_delta": sum_expected,
            "sum_balances": sum_bal,
            "pass": bool(
                expected == self.total_events
                and business == expected
                and sink_unique == expected
                and pending == 0
                and violations == 0
                and sum_expected == sum_bal
            ),
        }
        self.invariants_file.write_text(json.dumps(inv, indent=2), encoding="utf-8")

        summary = {
            "pattern": "webhook_outbox",
            "run_id": self.artifact_dir.name,
            "artifact_dir": str(self.artifact_dir),
            "timestamp_utc": now_utc(),
            "total_events": self.total_events,
            "dup_every": self.dup_every,
            "sink_fail_pct": self.sink_fail_pct,
            "faults": self.metrics,
            "invariants_pass": inv["pass"],
            "invariants_file": str(self.invariants_file),
        }
        self.summary_file.write_text(json.dumps(summary, indent=2), encoding="utf-8")
        if not inv["pass"]:
            raise RuntimeError(f"invariants failed: {json.dumps(inv)}")


if __name__ == "__main__":
    random.seed(getenv_int("RANDOM_SEED", 42))
    c = Campaign()
    c.run()
