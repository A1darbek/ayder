#!/usr/bin/env python3
"""Live HTTP security/lifecycle regression suite for an Ayder binary.

The suite launches isolated one-worker instances with unique shared-memory
names. It intentionally uses raw sockets so duplicate headers, incomplete
messages, connection admission, and peer-close timing remain observable.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from typing import Callable, Iterable
import uuid


TOKEN = "revenue-readiness-live-token"
ALLOWED_ORIGIN = "https://console.example.test"

SANITIZER_MARKERS = (
    "ERROR: AddressSanitizer",
    "SUMMARY: AddressSanitizer",
    "UndefinedBehaviorSanitizer",
    "runtime error:",
)

CONTROLLED_ENV = (
    "AYDER_METRICS_SHM_NAME",
    "AYDER_STORAGE_SHM_NAME",
    "BP_MAX_ACTIVE_CONNS",
    "PORT",
    "RAMFORGE_WORKERS",
    "RF_AUTH_SKIP_PATHS",
    "RF_AUTH_SKIP_PREFIXES",
    "RF_CORS_ALLOWED_ORIGIN",
    "RF_ENABLE_TEST_HOOKS",
    "RF_HA_ENABLED",
    "RF_HTTP_ADDR",
    "RF_HTTP_BODY_TIMEOUT_MS",
    "RF_HTTP_FORCE_FOLLOWER",
    "RF_HTTP_HEADER_TIMEOUT_MS",
    "RF_HTTP_IDLE_TIMEOUT_MS",
    "RF_HTTP_LEADER_URL",
    "RF_HTTP_PORT",
    "RF_HTTP_READ_ONLY",
    "RF_HTTP_REQUEST_TIMEOUT_MS",
    "RF_METRICS_PUBLIC",
)


class TestFailure(RuntimeError):
    pass


@dataclass
class HttpResponse:
    status: int
    headers: dict[str, list[str]]
    body: bytes
    raw: bytes

    def values(self, name: str) -> list[str]:
        return self.headers.get(name.lower(), [])

    def value(self, name: str) -> str | None:
        values = self.values(name)
        return values[0] if values else None


@dataclass
class StopResult:
    returncode: int
    elapsed: float
    log: str


class Results:
    def __init__(self, label: str) -> None:
        self.label = label
        self.passed = 0
        self.failed = 0

    def case(self, name: str, test: Callable[[], str | None]) -> None:
        try:
            detail = test()
        except Exception as exc:  # keep independent live checks running
            self.failed += 1
            print(f"FAIL [{self.label}] {name}: {exc}", file=sys.stderr)
            return
        self.passed += 1
        suffix = f" ({detail})" if detail else ""
        print(f"PASS [{self.label}] {name}{suffix}")


def reserve_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def make_request(
    path: str,
    headers: Iterable[tuple[str, str]] = (),
    method: str = "GET",
    body: bytes = b"",
) -> bytes:
    header_list = list(headers)
    lower_names = {name.lower() for name, _ in header_list}
    if body and "content-length" not in lower_names:
        header_list.append(("Content-Length", str(len(body))))

    lines = [
        f"{method} {path} HTTP/1.1",
        "Host: 127.0.0.1",
        "Connection: close",
    ]
    lines.extend(f"{name}: {value}" for name, value in header_list)
    return ("\r\n".join(lines) + "\r\n\r\n").encode("ascii") + body


def parse_response(raw: bytes) -> HttpResponse:
    head, separator, body = raw.partition(b"\r\n\r\n")
    if not separator:
        raise TestFailure(f"response has no header terminator: {raw[:120]!r}")
    lines = head.split(b"\r\n")
    status_fields = lines[0].split(b" ")
    if len(status_fields) < 2 or not status_fields[1].isdigit():
        raise TestFailure(f"invalid status line: {lines[0]!r}")

    headers: dict[str, list[str]] = {}
    for line in lines[1:]:
        name, colon, value = line.partition(b":")
        if not colon:
            raise TestFailure(f"malformed response header: {line!r}")
        key = name.decode("latin-1").strip().lower()
        headers.setdefault(key, []).append(value.decode("latin-1").strip())

    content_length_values = headers.get("content-length", [])
    if content_length_values:
        try:
            expected = int(content_length_values[0])
        except ValueError as exc:
            raise TestFailure("non-decimal response Content-Length") from exc
        if len(body) != expected:
            raise TestFailure(
                f"response body length {len(body)} does not match Content-Length {expected}"
            )
    return HttpResponse(int(status_fields[1]), headers, body, raw)


def exchange(port: int, request: bytes, timeout: float = 3.0) -> HttpResponse:
    raw = bytearray()
    with socket.create_connection(("127.0.0.1", port), timeout=timeout) as conn:
        conn.settimeout(timeout)
        conn.sendall(request)
        while True:
            try:
                block = conn.recv(65536)
            except ConnectionResetError:
                if raw:
                    break
                raise
            if not block:
                break
            raw.extend(block)
            if len(raw) > 512 * 1024:
                raise TestFailure("response exceeded integration-test bound")
    return parse_response(bytes(raw))


def require(condition: bool, message: str) -> None:
    if not condition:
        raise TestFailure(message)


def require_status(response: HttpResponse, expected: int) -> str:
    require(
        response.status == expected,
        f"expected HTTP {expected}, received {response.status}",
    )
    return f"status={response.status}"


def require_unauthorized(response: HttpResponse) -> str:
    require_status(response, 401)
    challenge = response.value("www-authenticate")
    require(challenge is not None and challenge.lower().startswith("bearer "),
            "401 response is missing a Bearer WWW-Authenticate challenge")
    return "status=401, bearer_challenge=present"


def wait_for_peer_close(conn: socket.socket, timeout: float = 3.0) -> float:
    started = time.monotonic()
    conn.settimeout(0.1)
    received = bytearray()
    while time.monotonic() - started < timeout:
        try:
            block = conn.recv(4096)
        except socket.timeout:
            continue
        except (ConnectionResetError, ConnectionAbortedError):
            block = b""
        if not block:
            require(not received,
                    f"timeout connection unexpectedly received {bytes(received[:120])!r}")
            return time.monotonic() - started
        received.extend(block)
    raise TestFailure(f"peer did not close within {timeout:.1f}s")


class LiveServer:
    def __init__(
        self,
        binary: Path,
        phase: str,
        overrides: dict[str, str] | None = None,
        workers: int = 1,
        startup_timeout: float = 15.0,
    ) -> None:
        self.binary = binary
        self.phase = phase
        self.overrides = overrides or {}
        self.workers = workers
        self.startup_timeout = startup_timeout
        self.port = reserve_port()
        self.instance_id = uuid.uuid4().hex
        self.storage_name = f"/ayder_security_storage_{self.instance_id}"
        self.metrics_name = f"/ayder_security_metrics_{self.instance_id}"
        self.tempdir = tempfile.TemporaryDirectory(prefix="ayder-http-security-")
        self.log_path = Path(self.tempdir.name) / "server.log"
        self.log_file = None
        self.process: subprocess.Popen[bytes] | None = None
        self.stop_result: StopResult | None = None

    def environment(self) -> dict[str, str]:
        env = os.environ.copy()
        for name in CONTROLLED_ENV:
            env.pop(name, None)
        env.update(
            {
                "AYDER_METRICS_SHM_NAME": self.metrics_name,
                "AYDER_STORAGE_SHM_NAME": self.storage_name,
                "RF_BEARER_TOKENS": TOKEN,
                "RF_BUS_DESC": "4096",
                "RF_BUS_HEAP_MB": "32",
                "RF_HTTP_DISABLE_RL": "1",
                "RF_SHARED_ENTRIES": "65536",
            }
        )
        env.update(self.overrides)
        return env

    def launch(self) -> None:
        require(self.process is None, "server was launched twice")
        self.log_file = self.log_path.open("wb")
        command = [
            str(self.binary),
            "--aof",
            "never",
            "--port",
            str(self.port),
            "--workers",
            str(self.workers),
        ]
        self.process = subprocess.Popen(
            command,
            cwd=self.tempdir.name,
            env=self.environment(),
            stdout=self.log_file,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )

    def start(self) -> "LiveServer":
        self.launch()
        deadline = time.monotonic() + self.startup_timeout
        last_error: Exception | None = None
        while time.monotonic() < deadline:
            if self.process is not None and self.process.poll() is not None:
                raise TestFailure(
                    f"{self.phase} server exited during startup with "
                    f"{self.process.returncode}: {self.log_text()[-1200:]}"
                )
            try:
                response = exchange(self.port, make_request("/health"), timeout=0.4)
                if response.status == 200:
                    time.sleep(0.15)  # allow the readiness connection close callback to run
                    return self
                last_error = TestFailure(f"readiness returned {response.status}")
            except (OSError, TestFailure) as exc:
                last_error = exc
            time.sleep(0.05)
        raise TestFailure(f"{self.phase} server did not become ready: {last_error}")

    def log_text(self) -> str:
        if self.log_file is not None:
            self.log_file.flush()
        try:
            return self.log_path.read_text(encoding="utf-8", errors="replace")
        except FileNotFoundError:
            return ""

    def _cleanup_resources(self) -> None:
        if self.log_file is not None:
            self.log_file.close()
            self.log_file = None
        for name in (self.storage_name, self.metrics_name):
            path = Path("/dev/shm") / name.removeprefix("/")
            try:
                path.unlink()
            except FileNotFoundError:
                pass
        self.tempdir.cleanup()

    def stop(self, timeout: float = 10.0) -> StopResult:
        if self.stop_result is not None:
            return self.stop_result
        require(self.process is not None, "server was never launched")

        started = time.monotonic()
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGTERM)
            try:
                self.process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                try:
                    os.killpg(self.process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                self.process.wait(timeout=3.0)
        elapsed = time.monotonic() - started
        log = self.log_text()
        self.stop_result = StopResult(int(self.process.returncode), elapsed, log)
        self._cleanup_resources()
        return self.stop_result

    def finish_exited(self) -> StopResult:
        require(self.process is not None, "server was never launched")
        require(self.process.poll() is not None, "server is still running")
        log = self.log_text()
        self.stop_result = StopResult(int(self.process.returncode), 0.0, log)
        self._cleanup_resources()
        return self.stop_result

    def abort(self) -> None:
        if self.stop_result is not None:
            return
        try:
            self.stop(timeout=3.0)
        except Exception:
            if self.process is not None and self.process.poll() is None:
                try:
                    os.killpg(self.process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                self.process.wait(timeout=3.0)
            self._cleanup_resources()


def auth_header() -> tuple[str, str]:
    return ("Authorization", f"Bearer {TOKEN}")


def policy_phase(results: Results, binary: Path, sanitized: bool) -> None:
    server = LiveServer(binary, "policy-default")
    linger: socket.socket | None = None
    try:
        server.start()

        results.case(
            "public health remains unauthenticated",
            lambda: require_status(exchange(server.port, make_request("/health")), 200),
        )
        results.case(
            "protected KV endpoint rejects missing token",
            lambda: require_unauthorized(
                exchange(server.port, make_request("/kv/security-live"))
            ),
        )
        results.case(
            "metrics endpoint is private by default",
            lambda: require_unauthorized(
                exchange(server.port, make_request("/metrics"))
            ),
        )
        results.case(
            "HA metrics endpoint is private by default",
            lambda: require_unauthorized(
                exchange(server.port, make_request("/metrics_ha"))
            ),
        )
        results.case(
            "valid Bearer token reaches protected metrics",
            lambda: require_status(
                exchange(server.port, make_request("/metrics", [auth_header()])), 200
            ),
        )
        results.case(
            "Bearer scheme without whitespace is rejected",
            lambda: require_unauthorized(
                exchange(
                    server.port,
                    make_request(
                        "/metrics",
                        [("Authorization", f"Bearer{TOKEN}")],
                    ),
                )
            ),
        )
        results.case(
            "duplicate Authorization headers are rejected",
            lambda: require_unauthorized(
                exchange(
                    server.port,
                    make_request("/metrics", [auth_header(), auth_header()]),
                )
            ),
        )

        def default_cors() -> str:
            response = exchange(
                server.port,
                make_request("/health", [("Origin", ALLOWED_ORIGIN)]),
            )
            require_status(response, 200)
            require(not response.values("access-control-allow-origin"),
                    "default response emitted Access-Control-Allow-Origin")
            return "allow-origin=absent"

        results.case("CORS default emits no allow-origin header", default_cors)

        def disabled_debug_hook() -> str:
            response = exchange(
                server.port,
                make_request(
                    "/kv/security-debug-hook",
                    [auth_header(), ("X-Force-Overload", "1")],
                ),
            )
            require(response.status != 503,
                    "X-Force-Overload changed behavior while test hooks were disabled")
            require(response.status != 401, "valid token was unexpectedly rejected")
            return f"status={response.status}, overload_hook=inactive"

        results.case("debug overload hook is disabled by default", disabled_debug_hook)

        linger = socket.create_connection(("127.0.0.1", server.port), timeout=2.0)
        linger.sendall(b"GET /health HTTP/1.1\r\nHost: incomplete")
        stop = server.stop()

        def graceful_sigterm() -> str:
            require(stop.returncode == 0,
                    f"SIGTERM shutdown returned {stop.returncode}")
            require(stop.elapsed <= 10.0,
                    f"SIGTERM shutdown took {stop.elapsed:.2f}s")
            require("Cluster shutdown complete" in stop.log,
                    "cluster completion marker is absent")
            require("Parent exiting (cluster stopped)" in stop.log,
                    "parent cleanup marker is absent")
            return f"exit=0, elapsed={stop.elapsed:.2f}s"

        results.case("SIGTERM performs bounded cluster shutdown", graceful_sigterm)

        def shutdown_closes_connections() -> str:
            require(linger is not None, "linger socket was not created")
            elapsed = wait_for_peer_close(linger, timeout=1.0)
            return f"incomplete_connection_closed_in={elapsed:.3f}s"

        results.case("SIGTERM closes incomplete client connections", shutdown_closes_connections)

        def listener_is_closed() -> str:
            try:
                conn = socket.create_connection(("127.0.0.1", server.port), timeout=0.3)
            except OSError:
                return "listener=closed"
            conn.close()
            raise TestFailure("listener still accepted connections after shutdown")

        results.case("listener is closed after SIGTERM", listener_is_closed)
        check_sanitizer_log(stop.log, sanitized, "policy-default")
    finally:
        if linger is not None:
            linger.close()
        server.abort()


def cors_phase(results: Results, binary: Path, sanitized: bool) -> None:
    server = LiveServer(
        binary,
        "cors-allowlist",
        {"RF_CORS_ALLOWED_ORIGIN": ALLOWED_ORIGIN},
    )
    try:
        server.start()

        def allowed_origin() -> str:
            response = exchange(
                server.port,
                make_request("/health", [("Origin", ALLOWED_ORIGIN)]),
            )
            require_status(response, 200)
            require(response.values("access-control-allow-origin") == [ALLOWED_ORIGIN],
                    "allowed origin was not reflected exactly once")
            require(response.value("vary") == "Origin", "Vary: Origin is absent")
            require(response.value("access-control-allow-origin") != "*",
                    "wildcard CORS was emitted")
            return f"allow-origin={ALLOWED_ORIGIN}"

        results.case("explicit CORS allowlist emits exact origin", allowed_origin)

        def denied_origin() -> str:
            evil = ALLOWED_ORIGIN + ".evil"
            response = exchange(
                server.port,
                make_request("/health", [("Origin", evil)]),
            )
            require_status(response, 200)
            require(not response.values("access-control-allow-origin"),
                    "non-allowlisted origin received CORS permission")
            return "suffix-origin=denied"

        results.case("CORS allowlist rejects suffix origin", denied_origin)

        def cors_does_not_bypass_auth() -> str:
            response = exchange(
                server.port,
                make_request("/metrics", [("Origin", ALLOWED_ORIGIN)]),
            )
            require_unauthorized(response)
            require(response.value("access-control-allow-origin") == ALLOWED_ORIGIN,
                    "allowlisted 401 did not carry the explicit CORS policy")
            return "status=401, allow-origin=exact"

        results.case("CORS allowlist does not bypass metrics auth", cors_does_not_bypass_auth)
        stop = server.stop()
        require(stop.returncode == 0, "CORS phase did not shut down cleanly")
        check_sanitizer_log(stop.log, sanitized, "cors-allowlist")
    finally:
        server.abort()


def timeout_close_case(
    port: int,
    first: bytes | None,
    second: bytes | None = None,
    delay: float = 0.0,
) -> str:
    with socket.create_connection(("127.0.0.1", port), timeout=2.0) as conn:
        started = time.monotonic()
        if first is not None:
            conn.sendall(first)
        if second is not None:
            time.sleep(delay)
            conn.sendall(second)
        elapsed_wait = wait_for_peer_close(conn, timeout=3.0)
        elapsed = time.monotonic() - started
    require(elapsed >= 0.10, f"connection closed too early ({elapsed:.3f}s)")
    require(elapsed <= 3.2, f"connection timeout was too slow ({elapsed:.3f}s)")
    return f"closed_in={elapsed:.3f}s, wait={elapsed_wait:.3f}s"


def timeout_phase(results: Results, binary: Path, sanitized: bool) -> None:
    server = LiveServer(
        binary,
        "message-timeouts",
        {
            "RF_HTTP_BODY_TIMEOUT_MS": "200",
            "RF_HTTP_HEADER_TIMEOUT_MS": "200",
            "RF_HTTP_IDLE_TIMEOUT_MS": "200",
            "RF_HTTP_REQUEST_TIMEOUT_MS": "5000",
        },
    )
    try:
        server.start()
        results.case(
            "slow incomplete header is closed by header timeout",
            lambda: timeout_close_case(
                server.port,
                b"GET /health HTTP/1.1\r\n",
                b"Host: incomplete",
                0.12,
            ),
        )
        results.case(
            "incomplete fixed body is closed by body timeout",
            lambda: timeout_close_case(
                server.port,
                b"GET /health HTTP/1.1\r\nHost: local\r\n"
                b"Content-Length: 5\r\n\r\na",
            ),
        )
        results.case(
            "silent accepted socket is closed by idle timeout",
            lambda: timeout_close_case(server.port, None),
        )
        stop = server.stop()
        require(stop.returncode == 0, "timeout phase did not shut down cleanly")
        check_sanitizer_log(stop.log, sanitized, "message-timeouts")
    finally:
        server.abort()


def request_timeout_phase(results: Results, binary: Path, sanitized: bool) -> None:
    server = LiveServer(
        binary,
        "request-deadline",
        {
            "RF_HTTP_BODY_TIMEOUT_MS": "5000",
            "RF_HTTP_HEADER_TIMEOUT_MS": "5000",
            "RF_HTTP_IDLE_TIMEOUT_MS": "5000",
            "RF_HTTP_REQUEST_TIMEOUT_MS": "200",
        },
    )
    try:
        server.start()
        results.case(
            "request deadline bounds an otherwise long header timeout",
            lambda: timeout_close_case(
                server.port,
                b"GET /health HTTP/1.1\r\nHost: incomplete",
            ),
        )
        stop = server.stop()
        require(stop.returncode == 0, "request-timeout phase did not shut down cleanly")
        check_sanitizer_log(stop.log, sanitized, "request-deadline")
    finally:
        server.abort()


def connection_limit_phase(results: Results, binary: Path, sanitized: bool) -> None:
    server = LiveServer(
        binary,
        "connection-limit",
        {
            "BP_MAX_ACTIVE_CONNS": "2",
            "RF_HTTP_IDLE_TIMEOUT_MS": "10000",
        },
    )
    held: list[socket.socket] = []
    try:
        server.start()
        time.sleep(0.25)
        held.append(socket.create_connection(("127.0.0.1", server.port), timeout=2.0))
        held.append(socket.create_connection(("127.0.0.1", server.port), timeout=2.0))
        time.sleep(0.15)

        def excess_connection_rejected() -> str:
            with socket.create_connection(("127.0.0.1", server.port), timeout=2.0) as excess:
                elapsed = wait_for_peer_close(excess, timeout=1.5)
            return f"third_connection_closed_in={elapsed:.3f}s"

        results.case("active connection limit rejects excess socket",
                     excess_connection_rejected)

        for conn in held:
            conn.close()
        held.clear()

        def capacity_recovers() -> str:
            deadline = time.monotonic() + 3.0
            last_error: Exception | None = None
            while time.monotonic() < deadline:
                try:
                    response = exchange(server.port, make_request("/health"), timeout=0.5)
                    if response.status == 200:
                        return "capacity=recovered, status=200"
                    last_error = TestFailure(f"HTTP {response.status}")
                except (OSError, TestFailure) as exc:
                    last_error = exc
                time.sleep(0.05)
            raise TestFailure(f"connection capacity did not recover: {last_error}")

        results.case("connection capacity recovers after close", capacity_recovers)
        stop = server.stop()
        require(stop.returncode == 0, "connection-limit phase did not shut down cleanly")
        check_sanitizer_log(stop.log, sanitized, "connection-limit")
    finally:
        for conn in held:
            conn.close()
        server.abort()


def single_process_shutdown_phase(
    results: Results, binary: Path, sanitized: bool
) -> None:
    """Exercise the worker's cooperative libuv shutdown, not only the manager."""
    server = LiveServer(binary, "single-process-sigterm", workers=0)
    linger: socket.socket | None = None
    try:
        server.start()
        linger = socket.create_connection(("127.0.0.1", server.port), timeout=2.0)
        linger.sendall(b"GET /health HTTP/1.1\r\nHost: incomplete")
        stop = server.stop()

        def graceful_worker_shutdown() -> str:
            require(stop.returncode == 0,
                    f"single-process SIGTERM returned {stop.returncode}")
            require(stop.elapsed <= 3.0,
                    f"cooperative HTTP shutdown took {stop.elapsed:.2f}s")
            require("Single process mode caught signal 15" in stop.log,
                    "single-process SIGTERM handler marker is absent")
            require("shutting down gracefully" in stop.log,
                    "graceful worker shutdown marker is absent")
            return f"exit=0, elapsed={stop.elapsed:.2f}s"

        results.case("single-process SIGTERM exits cooperatively",
                     graceful_worker_shutdown)

        def graceful_worker_closes_connection() -> str:
            require(linger is not None, "linger socket was not created")
            elapsed = wait_for_peer_close(linger, timeout=1.0)
            return f"incomplete_connection_closed_in={elapsed:.3f}s"

        results.case("cooperative shutdown closes active connection",
                     graceful_worker_closes_connection)

        def graceful_worker_closes_listener() -> str:
            try:
                conn = socket.create_connection(("127.0.0.1", server.port), timeout=0.3)
            except OSError:
                return "listener=closed"
            conn.close()
            raise TestFailure("single-process listener remained open after SIGTERM")

        results.case("cooperative shutdown closes listener",
                     graceful_worker_closes_listener)
        check_sanitizer_log(stop.log, sanitized, "single-process-sigterm")
    finally:
        if linger is not None:
            linger.close()
        server.abort()


def invalid_timeout_phase(results: Results, binary: Path, sanitized: bool) -> None:
    server = LiveServer(
        binary,
        "invalid-timeout",
        {"RF_HTTP_HEADER_TIMEOUT_MS": "0"},
        workers=0,
    )
    result: StopResult | None = None
    try:
        server.launch()

        def invalid_timeout_rejected() -> str:
            require(server.process is not None, "invalid-timeout process was not launched")
            try:
                server.process.wait(timeout=8.0)
            except subprocess.TimeoutExpired as exc:
                raise TestFailure("server stayed alive with an invalid timeout") from exc
            nonlocal result
            result = server.finish_exited()
            require(result.returncode != 0,
                    "invalid timeout configuration exited successfully")
            require("Invalid RF_HTTP_HEADER_TIMEOUT_MS" in result.log,
                    "invalid-timeout diagnostic is absent")
            check_sanitizer_log(result.log, sanitized, "invalid-timeout")
            return f"exit={result.returncode}, diagnostic=present"

        results.case("invalid zero timeout fails startup", invalid_timeout_rejected)
    finally:
        if result is None:
            server.abort()


def check_sanitizer_log(log: str, sanitized: bool, phase: str) -> None:
    if not sanitized:
        return
    markers = [marker for marker in SANITIZER_MARKERS if marker in log]
    require(not markers, f"{phase} emitted sanitizer diagnostics: {markers}")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", required=True, type=Path,
                        help="Ayder executable to exercise")
    parser.add_argument("--label", default="live-security")
    parser.add_argument("--sanitized", action="store_true",
                        help="fail if ASan/UBSan diagnostics appear in server logs")
    args = parser.parse_args()

    binary = args.server.expanduser().resolve()
    if not binary.is_file() or not os.access(binary, os.X_OK):
        print(f"server binary is not executable: {binary}", file=sys.stderr)
        return 2

    results = Results(args.label)
    try:
        policy_phase(results, binary, args.sanitized)
        cors_phase(results, binary, args.sanitized)
        timeout_phase(results, binary, args.sanitized)
        request_timeout_phase(results, binary, args.sanitized)
        connection_limit_phase(results, binary, args.sanitized)
        single_process_shutdown_phase(results, binary, args.sanitized)
        invalid_timeout_phase(results, binary, args.sanitized)
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        return 130
    except Exception as exc:
        results.failed += 1
        print(f"FAIL [{args.label}] suite infrastructure: {exc}", file=sys.stderr)

    total = results.passed + results.failed
    print(
        f"http_live_security_test[{args.label}]: "
        f"{results.passed}/{total} passed, {results.failed} failed"
    )
    return 1 if results.failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
