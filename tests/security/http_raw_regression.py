#!/usr/bin/env python3
"""Adversarial HTTP/1.1 framing checks against a running Ayder server."""

from __future__ import annotations

import argparse
import socket
import sys
import time
from dataclasses import dataclass


@dataclass(frozen=True)
class Case:
    name: str
    request: bytes
    expected_status: int
    fragment_size: int = 0


def request(method: bytes, framing: bytes = b"", body: bytes = b"") -> bytes:
    return (
        method
        + b" /health HTTP/1.1\r\n"
        + b"Host: localhost\r\n"
        + b"Connection: close\r\n"
        + framing
        + b"\r\n"
        + body
    )


CASES = (
    Case("baseline", request(b"GET"), 200),
    Case(
        "header_count_exhaustion",
        request(b"POST", b"".join(b"X-Test: x\r\n" for _ in range(40))),
        400,
    ),
    Case(
        "oversized_header",
        request(b"POST", b"X-Oversized: " + b"x" * 65536 + b"\r\n"),
        400,
    ),
    Case(
        "te_then_cl",
        request(
            b"POST",
            b"Transfer-Encoding: chunked\r\nContent-Length: 4\r\n",
            b"0\r\n\r\n",
        ),
        400,
    ),
    Case(
        "cl_then_te",
        request(
            b"POST",
            b"Content-Length: 4\r\nTransfer-Encoding: chunked\r\n",
            b"0\r\n\r\n",
        ),
        400,
    ),
    Case(
        "duplicate_cl_mismatch",
        request(
            b"POST", b"Content-Length: 3\r\nContent-Length: 4\r\n", b"abc"
        ),
        400,
    ),
    Case(
        "duplicate_cl_equal",
        request(b"GET", b"Content-Length: 3\r\nContent-Length: 3\r\n", b"abc"),
        200,
    ),
    Case(
        "duplicate_cl_nonidentical_representation",
        request(b"POST", b"Content-Length: 03\r\nContent-Length: 3\r\n", b"abc"),
        400,
    ),
    Case("empty_cl", request(b"POST", b"Content-Length:\r\n"), 400),
    Case("signed_cl", request(b"POST", b"Content-Length: +3\r\n", b"abc"), 400),
    Case("suffix_cl", request(b"POST", b"Content-Length: 3x\r\n", b"abc"), 400),
    Case("comma_cl", request(b"POST", b"Content-Length: 3, 3\r\n", b"abc"), 400),
    Case(
        "overflow_cl",
        request(b"POST", b"Content-Length: 184467440737095516160\r\n"),
        400,
    ),
    Case(
        "oversized_cl",
        request(b"POST", b"Content-Length: 65536\r\n"),
        400,
    ),
    Case("unsupported_te", request(b"POST", b"Transfer-Encoding: gzip\r\n"), 400),
    Case(
        "te_list",
        request(b"POST", b"Transfer-Encoding: gzip, chunked\r\n"),
        400,
    ),
    Case(
        "te_prefix",
        request(b"POST", b"Transfer-Encoding: chunkedx\r\n"),
        400,
    ),
    Case(
        "duplicate_te",
        request(
            b"POST",
            b"Transfer-Encoding: chunked\r\nTransfer-Encoding: chunked\r\n",
            b"0\r\n\r\n",
        ),
        400,
    ),
    Case(
        "chunk_same_read",
        request(b"GET", b"Transfer-Encoding: Chunked\r\n", b"3\r\nabc\r\n0\r\n\r\n"),
        200,
    ),
    Case(
        "content_length_reallocation",
        request(b"GET", b"Content-Length: 5000\r\n", b"x" * 5000),
        200,
        1024,
    ),
    Case(
        "chunk_reallocation",
        request(
            b"GET",
            b"Transfer-Encoding: chunked\r\n",
            b"1388\r\n" + b"x" * 5000 + b"\r\n0\r\n\r\n",
        ),
        200,
        1024,
    ),
    Case(
        "chunk_fragmented",
        request(
            b"GET",
            b"Transfer-Encoding: chunked\r\n",
            b"3;name=value\r\nabc\r\n0\r\nTrailer: yes\r\n\r\n",
        ),
        200,
        1,
    ),
    Case(
        "malformed_chunk_size",
        request(b"POST", b"Transfer-Encoding: chunked\r\n", b"1x\r\na\r\n0\r\n\r\n"),
        400,
    ),
    Case(
        "overflow_chunk_size",
        request(
            b"POST",
            b"Transfer-Encoding: chunked\r\n",
            b"10000000000000000\r\n",
        ),
        400,
    ),
    Case(
        "oversized_chunk",
        request(b"POST", b"Transfer-Encoding: chunked\r\n", b"10000\r\n"),
        400,
    ),
    Case(
        "fixed_body_leftover",
        request(b"POST", b"Content-Length: 1\r\n", b"ab"),
        400,
    ),
    Case(
        "chunked_leftover_pipeline",
        request(
            b"POST",
            b"Transfer-Encoding: chunked\r\n",
            b"0\r\n\r\nGET /health HTTP/1.1\r\nHost: localhost\r\n\r\n",
        ),
        400,
    ),
)


def run_case(host: str, port: int, timeout: float, case: Case) -> None:
    with socket.create_connection((host, port), timeout=timeout) as conn:
        conn.settimeout(timeout)
        if case.fragment_size:
            for offset in range(0, len(case.request), case.fragment_size):
                conn.sendall(case.request[offset : offset + case.fragment_size])
                time.sleep(0.001)
        else:
            conn.sendall(case.request)

        response = bytearray()
        while b"\r\n" not in response and len(response) < 4096:
            block = conn.recv(4096)
            if not block:
                break
            response.extend(block)

    status_line = bytes(response).split(b"\r\n", 1)[0]
    fields = status_line.split(b" ")
    if len(fields) < 2 or not fields[1].isdigit():
        raise AssertionError(f"{case.name}: invalid response {status_line!r}")
    status = int(fields[1])
    if status != case.expected_status:
        raise AssertionError(
            f"{case.name}: expected {case.expected_status}, got {status}: {status_line!r}"
        )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", default=1163, type=int)
    parser.add_argument("--timeout", default=3.0, type=float)
    args = parser.parse_args()

    failures = []
    for case in CASES:
        try:
            run_case(args.host, args.port, args.timeout, case)
            print(f"PASS {case.name}")
        except (AssertionError, OSError) as exc:
            failures.append(str(exc))
            print(f"FAIL {exc}", file=sys.stderr)

    if failures:
        print(f"http_raw_regression: {len(failures)}/{len(CASES)} failed", file=sys.stderr)
        return 1
    print(f"http_raw_regression: {len(CASES)} cases passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
