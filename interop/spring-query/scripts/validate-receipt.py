#!/usr/bin/env python3
import argparse
import json
import pathlib
import re
import sys
from collections import Counter
from typing import Any


SHA256_FINGERPRINT = re.compile(r"^sha256:[0-9a-f]{64}$")


def is_fingerprint(value: Any) -> bool:
    return isinstance(value, str) and SHA256_FINGERPRINT.fullmatch(value) is not None


def is_etag(value: Any) -> bool:
    return isinstance(value, str) and bool(value) and value != "null"


def is_non_negative_integer(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def load_json(path: pathlib.Path) -> dict[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as receipt_file:
            value = json.load(receipt_file)
    except (OSError, json.JSONDecodeError) as exc:
        raise SystemExit(f"{path}: cannot read receipt: {exc}") from exc
    if not isinstance(value, dict):
        raise SystemExit(f"{path}: receipt root must be a JSON object")
    return value


def validate_receipt(path: pathlib.Path) -> list[str]:
    receipt = load_json(path)
    expectations_path = pathlib.Path(__file__).resolve().parents[1] / "expected" / "expectations.json"
    expectations = load_json(expectations_path)
    errors: list[str] = []

    rows = receipt.get("rows")
    if not isinstance(rows, list):
        return ["receipt.rows must be an array"]

    allowed_statuses = set(expectations.get("statuses", []))
    required_ids = set(expectations.get("row_ids", []))
    if receipt.get("target") == "ayder-envoy":
        required_ids.add("transport.direct_envoy_equivalence")

    row_ids = [row.get("id") for row in rows if isinstance(row, dict)]
    counts = Counter(row_ids)
    duplicate_ids = sorted(str(row_id) for row_id, count in counts.items() if count > 1)
    if duplicate_ids:
        errors.append(f"duplicate row IDs: {', '.join(duplicate_ids)}")

    missing_ids = sorted(required_ids - set(row_ids))
    if missing_ids:
        errors.append(f"missing required row IDs: {', '.join(missing_ids)}")

    rows_by_id: dict[str, dict[str, Any]] = {}
    for index, row in enumerate(rows):
        if not isinstance(row, dict):
            errors.append(f"rows[{index}] must be an object")
            continue
        row_id = row.get("id")
        if not isinstance(row_id, str) or not row_id:
            errors.append(f"rows[{index}] has a missing or invalid id")
            continue
        rows_by_id.setdefault(row_id, row)
        result = row.get("result")
        if result not in allowed_statuses:
            errors.append(f"{row_id}: result {result!r} is outside the allowed status set")
        if result == "FAIL":
            errors.append(f"{row_id}: receipt contains FAIL")

    content_location = rows_by_id.get("representation.content_location")
    if content_location and content_location.get("result") != "NOT_SUPPORTED":
        errors.append("representation.content_location must be NOT_SUPPORTED")

    repeat = rows_by_id.get("representation.repeat_identical_query")
    if repeat and repeat.get("result") == "PASS":
        first_fp = repeat.get("first_fingerprint")
        repeat_fp = repeat.get("repeat_fingerprint")
        first_etag = repeat.get("first_etag")
        repeat_etag = repeat.get("repeat_etag")
        if not (
            repeat.get("observed_status") == 200
            and is_fingerprint(first_fp)
            and is_fingerprint(repeat_fp)
            and first_fp == repeat_fp
            and is_etag(first_etag)
            and is_etag(repeat_etag)
            and first_etag == repeat_etag
        ):
            errors.append(
                "representation.repeat_identical_query: PASS requires status 200, "
                "equal valid fingerprints, and equal non-empty ETags"
            )

    equivalent = rows_by_id.get("representation.equivalent_json")
    if equivalent and equivalent.get("result") == "PASS":
        first_fp = equivalent.get("first_fingerprint")
        equivalent_fp = equivalent.get("equivalent_fingerprint")
        first_rows = equivalent.get("first_rows")
        equivalent_rows = equivalent.get("equivalent_rows")
        if not (
            equivalent.get("first_status") == 200
            and equivalent.get("equivalent_status") == 200
            and is_non_negative_integer(first_rows)
            and first_rows > 0
            and first_rows == equivalent_rows
            and is_fingerprint(first_fp)
            and is_fingerprint(equivalent_fp)
            and first_fp == equivalent_fp
        ):
            errors.append(
                "representation.equivalent_json: PASS requires two status-200, "
                "non-empty equal-row results with equal valid fingerprints"
            )

    committed_offset = rows_by_id.get("safety.committed_offset")
    if committed_offset and committed_offset.get("result") == "PASS":
        before = committed_offset.get("before")
        after = committed_offset.get("after")
        if not (
            is_non_negative_integer(before)
            and is_non_negative_integer(after)
            and before == after
        ):
            errors.append(
                "safety.committed_offset: PASS requires equal numeric non-negative offsets"
            )

    broker_state = rows_by_id.get("safety.broker_state")
    if broker_state and broker_state.get("result") == "PASS":
        equal_pairs = (
            ("high_watermark_before", "high_watermark_after"),
            ("message_count_before", "message_count_after"),
            ("idempotency_records_before", "idempotency_records_after"),
        )
        if broker_state.get("broker_state_mutated") is not False or any(
            not is_number(broker_state.get(before))
            or not is_number(broker_state.get(after))
            or broker_state.get(before) != broker_state.get(after)
            for before, after in equal_pairs
        ):
            errors.append(
                "safety.broker_state: PASS requires broker_state_mutated=false "
                "and equal numeric before/after counters"
            )

    transport = rows_by_id.get("transport.direct_envoy_equivalence")
    if transport and transport.get("result") == "PASS":
        direct = transport.get("direct")
        through_envoy = transport.get("through_envoy")
        valid = isinstance(direct, dict) and isinstance(through_envoy, dict)
        if valid:
            direct_fp = direct.get("fingerprint")
            envoy_fp = through_envoy.get("fingerprint")
            direct_etag = direct.get("etag")
            envoy_etag = through_envoy.get("etag")
            valid = (
                direct.get("status") == 200
                and through_envoy.get("status") == 200
                and is_fingerprint(direct_fp)
                and is_fingerprint(envoy_fp)
                and direct_fp == envoy_fp
                and is_etag(direct_etag)
                and is_etag(envoy_etag)
                and direct_etag == envoy_etag
            )
        if not valid:
            errors.append(
                "transport.direct_envoy_equivalence: PASS requires status-200 "
                "direct/Envoy evidence with equal valid fingerprints and non-empty ETags"
            )

    rate_limit = rows_by_id.get("ayder.rate_limit_headers_sane")
    if rate_limit and rate_limit.get("result") == "PASS":
        limit = rate_limit.get("limit")
        remaining = rate_limit.get("remaining")
        reset = rate_limit.get("reset")
        if not (
            is_non_negative_integer(limit)
            and is_non_negative_integer(remaining)
            and is_non_negative_integer(reset)
            and limit > 0
            and remaining <= limit
            and reset > 0
        ):
            errors.append(
                "ayder.rate_limit_headers_sane: PASS requires positive limit/reset "
                "and non-negative remaining no greater than limit"
            )

    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate Ayder QUERY interop receipt evidence.")
    parser.add_argument("receipt", type=pathlib.Path)
    args = parser.parse_args()

    errors = validate_receipt(args.receipt)
    if errors:
        for error in errors:
            print(f"{args.receipt}: {error}", file=sys.stderr)
        return 1

    print(f"{args.receipt}: valid")
    return 0


if __name__ == "__main__":
    sys.exit(main())
