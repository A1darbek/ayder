#!/usr/bin/env python3
import argparse
import datetime as dt
import json
import pathlib
import sys


ORDER = {
    "PASS": 0,
    "FAIL": 1,
    "NOT_SUPPORTED": 2,
    "NOT_APPLICABLE": 3,
    "UNVERIFIED": 4,
    "OBSERVED": 5,
}


def read_rows(path: pathlib.Path):
    rows = []
    with path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                raise SystemExit(f"{path}:{line_no}: invalid JSON: {exc}") from exc
            rows.append(row)
    return rows


def summarize(rows):
    summary = {
        "pass": 0,
        "fail": 0,
        "not_supported": 0,
        "not_applicable": 0,
        "unverified": 0,
        "observed": 0,
    }
    for row in rows:
        status = row.get("result", "UNVERIFIED")
        key = status.lower()
        if key in summary:
            summary[key] += 1
        else:
            summary["unverified"] += 1
    return summary


def sort_rows(rows):
    return sorted(rows, key=lambda r: (ORDER.get(r.get("result", "UNVERIFIED"), 99), r.get("id", "")))


def write_text(receipt, path: pathlib.Path):
    rows = receipt["rows"]
    summary = receipt["summary"]
    lines = []
    lines.append("============================================================")
    lines.append(" AYDER RFC 10008 QUERY INTEROP RECEIPT")
    lines.append("============================================================")
    lines.append(f" target       : {receipt['target']}")
    lines.append(f" generated_at : {receipt['generated_at']}")
    lines.append(f" commit       : {receipt['implementation_commit']}")
    lines.append(f" base_url     : {receipt['base_url']}")
    lines.append("------------------------------------------------------------")
    lines.append(
        " summary      : "
        f"PASS={summary['pass']} "
        f"FAIL={summary['fail']} "
        f"OBSERVED={summary['observed']} "
        f"UNVERIFIED={summary['unverified']} "
        f"NOT_SUPPORTED={summary['not_supported']} "
        f"NOT_APPLICABLE={summary['not_applicable']}"
    )
    lines.append("------------------------------------------------------------")
    for row in rows:
        lines.append(f" {row.get('result', 'UNVERIFIED'):<15} {row.get('id', '')}")
        detail = row.get("detail") or row.get("message")
        if detail:
            lines.append(f"   detail: {detail}")
        for key in (
            "expected_status",
            "observed_status",
            "fingerprint",
            "etag",
            "content_location",
            "limit",
            "remaining",
            "reset",
            "before",
            "after",
            "direct_fingerprint",
            "through_envoy_fingerprint",
        ):
            if key in row:
                lines.append(f"   {key}: {row[key]}")
    lines.append("============================================================")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main():
    parser = argparse.ArgumentParser(description="Render Ayder QUERY interop receipts.")
    parser.add_argument("--target", required=True)
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--rows", required=True, type=pathlib.Path)
    parser.add_argument("--out-dir", required=True, type=pathlib.Path)
    args = parser.parse_args()

    rows = read_rows(args.rows)
    receipt = {
        "schema_version": "0.1",
        "target": args.target,
        "generated_at": dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "implementation_commit": args.commit,
        "base_url": args.base_url,
        "rows": rows,
        "summary": summarize(rows),
    }

    args.out_dir.mkdir(parents=True, exist_ok=True)
    json_path = args.out_dir / "receipt.json"
    txt_path = args.out_dir / "receipt.txt"
    json_path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    write_text(receipt, txt_path)
    print(txt_path)


if __name__ == "__main__":
    sys.exit(main())
