#!/usr/bin/env python3
import csv
import json
import sys
from pathlib import Path


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: summarize_matrix.py MATRIX_OUTPUT_DIR", file=sys.stderr)
        return 2
    root = Path(sys.argv[1])
    rows = []
    for metadata_path in sorted(root.glob("*/metadata.json")):
        case_dir = metadata_path.parent
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        summary_path = case_dir / "summary.json"
        summary = json.loads(summary_path.read_text(encoding="utf-8")) if summary_path.exists() else {}
        status_path = case_dir / "exit_status"
        exit_status = int(status_path.read_text().strip()) if status_path.exists() else 1
        rows.append({"case": case_dir.name, "exit_status": exit_status, **metadata, **summary})
    if not rows:
        print(f"no completed matrix cases under {root}", file=sys.stderr)
        return 1
    (root / "matrix.json").write_text(json.dumps(rows, indent=2) + "\n", encoding="utf-8")
    fields = sorted({key for row in rows for key in row})
    with (root / "matrix.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)
    print(json.dumps({"case_count": len(rows), "all_cases_clean": all(r["exit_status"] == 0 for r in rows)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
