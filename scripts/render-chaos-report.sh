  #!/usr/bin/env bash
set -euo pipefail

ART_DIR="${1:-}"
if [ -z "${ART_DIR}" ]; then
  ART_DIR="$(ls -dt ./artifacts/chaos_* 2>/dev/null | head -n1 || true)"
fi

if [ -z "${ART_DIR}" ] || [ ! -d "${ART_DIR}" ]; then
  echo "Usage: $0 <artifacts/chaos_YYYYmmdd_HHMMSS>"
  echo "No artifact dir found."
  exit 1
fi

python3 - "$ART_DIR" <<'PY'
import json
import re
import sys
from pathlib import Path
from collections import defaultdict, Counter

art = Path(sys.argv[1]).resolve()
summary_dir = art / "summary"
summary_dir.mkdir(parents=True, exist_ok=True)

def read_text(p: Path) -> str:
    try:
        return p.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return ""

def parse_scalar_metric(text: str, name: str):
    m = re.search(rf'(?m)^{re.escape(name)}\s+([0-9eE+.\-]+)\s*$', text)
    if not m:
        return None
    raw = m.group(1)
    try:
        v = float(raw)
        if v.is_integer():
            return int(v)
        return v
    except Exception:
        return raw

def parse_labels(lbls: str):
    out = {}
    for k, v in re.findall(r'([A-Za-z_][A-Za-z0-9_]*)="([^"]*)"', lbls):
        out[k] = v
    return out

def parse_prom_metrics_file(p: Path):
    text = read_text(p)
    if "ramforge_ha_role" not in text:
        return None

    metrics = {
        "ramforge_ha_role": parse_scalar_metric(text, "ramforge_ha_role"),
        "ramforge_ha_state": parse_scalar_metric(text, "ramforge_ha_state"),
        "ramforge_ha_term": parse_scalar_metric(text, "ramforge_ha_term"),
        "ramforge_ha_commit_index": parse_scalar_metric(text, "ramforge_ha_commit_index"),
        "ramforge_ha_applied_index": parse_scalar_metric(text, "ramforge_ha_applied_index"),
        "ramforge_ha_replication_lag_ms": parse_scalar_metric(text, "ramforge_ha_replication_lag_ms"),
        "ramforge_ha_elections_total": parse_scalar_metric(text, "ramforge_ha_elections_total"),
        "ramforge_ha_heartbeats_sent_total": parse_scalar_metric(text, "ramforge_ha_heartbeats_sent_total"),
        "ramforge_ha_heartbeats_received_total": parse_scalar_metric(text, "ramforge_ha_heartbeats_received_total"),
        "ramforge_ha_writes_replicated_total": parse_scalar_metric(text, "ramforge_ha_writes_replicated_total"),
        "ramforge_ha_writes_failed_total": parse_scalar_metric(text, "ramforge_ha_writes_failed_total"),
    }

    node_info_rows = []
    for lbls, val in re.findall(r'(?m)^ramforge_ha_node_info\{([^}]*)\}\s+([0-9eE+.\-]+)\s*$', text):
        d = parse_labels(lbls)
        d["_value"] = float(val)
        node_info_rows.append(d)

    node_role_rows = []
    for lbls, val in re.findall(r'(?m)^ramforge_ha_node_role\{([^}]*)\}\s+([0-9eE+.\-]+)\s*$', text):
        d = parse_labels(lbls)
        try:
            d["_value"] = int(float(val))
        except Exception:
            d["_value"] = val
        node_role_rows.append(d)

    self_info = None
    for r in node_info_rows:
        if r.get("local") == "1":
            self_info = r
            break

    return {
        "metrics": metrics,
        "node_info": node_info_rows,
        "node_role_rows": node_role_rows,
        "self_node_id": self_info.get("node_id") if self_info else None,
        "self_leader_label": (self_info.get("leader") if self_info else None),
        "self_priority": (self_info.get("priority") if self_info else None),
        "raw_path": str(p),
    }

def role_name(v):
    return {0: "leader", 1: "follower", 2: "candidate", 3: "learner"}.get(v, str(v))

def state_name(v):
    return {0: "init", 1: "ready", 2: "syncing", 3: "degraded", 4: "failed"}.get(v, str(v))

def parse_legacy_tab_line(line: str):
    """
    Legacy format from older run-chaos-suite.sh:
      <json-prefix>\t<raw server json>
    Returns dict or raises.
    """
    left, right = line.split("\t", 1)
    base = json.loads(left)
    if not isinstance(base, dict):
        raise ValueError("legacy left part is not object")
    try:
        body = json.loads(right)
        base["response"] = body
        if isinstance(body, dict) and "ok" in body:
            base["ok"] = bool(body["ok"])
    except Exception:
        base["response_raw"] = right
    return base

def parse_producer_log_line(line: str):
    line = line.strip()
    if not line:
        return None
    # New format: pure NDJSON object
    try:
        obj = json.loads(line)
        if isinstance(obj, dict):
            return obj
    except Exception:
        pass
    # Back-compat: legacy tab-separated prefix + raw response JSON
    if "\t" in line:
        try:
            return parse_legacy_tab_line(line)
        except Exception:
            return None
    return None

# Discover metric snapshots
phase_data = defaultdict(dict)  # phase -> port -> parsed
metrics_root = art / "metrics"
if metrics_root.exists():
    for phase_dir in sorted([p for p in metrics_root.iterdir() if p.is_dir()]):
        phase = phase_dir.name
        for f in phase_dir.rglob("*"):
            if not f.is_file():
                continue
            parsed = parse_prom_metrics_file(f)
            if not parsed:
                continue
            m = re.search(r'port[_: ]([0-9]{2,5})', f.name)
            if not m:
                m = re.search(r'port[_: ]([0-9]{2,5})', str(f))
            port = m.group(1) if m else f.name
            phase_data[phase][port] = parsed

# Producer summary (supports both new NDJSON and legacy tab format)
producer_summary = {}
producer_candidates = list(art.rglob("producer_responses.ndjson"))
if producer_candidates:
    pf = producer_candidates[0]
    total = 0
    ok_true = 0
    ok_false = 0
    parse_errors = 0
    http_code_counts = Counter()
    target_port_counts = Counter()

    for raw_line in pf.read_text(encoding="utf-8", errors="replace").splitlines():
        if not raw_line.strip():
            continue
        total += 1
        obj = parse_producer_log_line(raw_line)
        if not obj:
            parse_errors += 1
            continue

        if obj.get("ok") is True:
            ok_true += 1
        elif obj.get("ok") is False:
            ok_false += 1

        hc = obj.get("http_code")
        if hc is not None:
            http_code_counts[str(hc)] += 1
        tp = obj.get("target_port")
        if tp is not None:
            target_port_counts[str(tp)] += 1

    producer_summary = {
        "file": str(pf.relative_to(art)),
        "total_lines": total,
        "ok_true": ok_true,
        "ok_false": ok_false,
        "json_parse_errors": parse_errors,
        "http_code_counts": dict(sorted(http_code_counts.items(), key=lambda kv: kv[0])),
        "target_port_counts": dict(sorted(target_port_counts.items(), key=lambda kv: int(kv[0]) if str(kv[0]).isdigit() else 999999)),
    }

# Index likely topic/partition related files
interesting_files = []
topic_partition_files_by_phase = defaultdict(lambda: defaultdict(list))  # phase -> port -> [files]
for f in art.rglob("*"):
    if not f.is_file():
        continue
    rel = str(f.relative_to(art))
    low = rel.lower()
    if any(k in low for k in ["topic", "topics", "partition", "partitions"]):
        interesting_files.append(rel)

        # Try to bucket by phase/port if under metrics/<phase>/port_<port>/
        m = re.match(r'^metrics/([^/]+)/port_([0-9]+)/(.+)$', rel)
        if m:
            phase, port, tail = m.groups()
            topic_partition_files_by_phase[phase][port].append(tail)

interesting_files = sorted(set(interesting_files))
for phase in topic_partition_files_by_phase:
    for port in topic_partition_files_by_phase[phase]:
        topic_partition_files_by_phase[phase][port] = sorted(set(topic_partition_files_by_phase[phase][port]))

# Build structured summary
summary = {
    "artifact_dir": str(art),
    "phases": {},
    "producer": producer_summary,
    "interesting_files": interesting_files,
    "topic_partition_files_by_phase": {
        phase: {port: files for port, files in sorted(ports.items(), key=lambda kv: int(kv[0]))}
        for phase, ports in sorted(topic_partition_files_by_phase.items())
    },
    "anomalies": [],
}

for phase, ports in sorted(phase_data.items()):
    phase_summary = {
        "ports": {},
        "leaders_reported_by_self": [],
        "followers_with_zero_heartbeats_received": [],
    }
    for port, parsed in sorted(ports.items(), key=lambda kv: int(kv[0]) if str(kv[0]).isdigit() else 99999):
        m = parsed["metrics"]
        self_node = parsed["self_node_id"]
        self_leader_label = parsed["self_leader_label"]
        row = {
            "self_node_id": self_node,
            "role": role_name(m.get("ramforge_ha_role")),
            "role_num": m.get("ramforge_ha_role"),
            "state": state_name(m.get("ramforge_ha_state")),
            "state_num": m.get("ramforge_ha_state"),
            "term": m.get("ramforge_ha_term"),
            "elections_total": m.get("ramforge_ha_elections_total"),
            "heartbeats_sent_total": m.get("ramforge_ha_heartbeats_sent_total"),
            "heartbeats_received_total": m.get("ramforge_ha_heartbeats_received_total"),
            "commit_index": m.get("ramforge_ha_commit_index"),
            "applied_index": m.get("ramforge_ha_applied_index"),
            "writes_replicated_total": m.get("ramforge_ha_writes_replicated_total"),
            "writes_failed_total": m.get("ramforge_ha_writes_failed_total"),
            "self_leader_label": self_leader_label,
            "file": str(Path(parsed["raw_path"]).relative_to(art)),
        }
        phase_summary["ports"][port] = row

        if self_leader_label == "1":
            phase_summary["leaders_reported_by_self"].append({"port": port, "node_id": self_node})
        if row["role"] != "leader" and (row["heartbeats_received_total"] in (0, 0.0, None)):
            phase_summary["followers_with_zero_heartbeats_received"].append({"port": port, "node_id": self_node})

    summary["phases"][phase] = phase_summary

# Heuristic anomalies
for phase, p in summary["phases"].items():
    ports = p["ports"]
    if not ports:
        continue
    nonleaders = [v for v in ports.values() if v["role"] != "leader"]
    if nonleaders and all((v["heartbeats_received_total"] in (0, 0.0, None)) for v in nonleaders):
        summary["anomalies"].append(
            f"{phase}: all non-leader nodes report heartbeats_received_total=0 (HA peer traffic likely not flowing)"
        )
    if all((v["writes_replicated_total"] in (0, 0.0, None)) for v in ports.values()):
        summary["anomalies"].append(
            f"{phase}: writes_replicated_total=0 on all nodes (replication path not observed)"
        )
    leaders = p["leaders_reported_by_self"]
    if len(leaders) == 0:
        summary["anomalies"].append(f"{phase}: no node self-reports leader")
    elif len(leaders) > 1:
        summary["anomalies"].append(f"{phase}: multiple nodes self-report leader ({leaders})")

# Render markdown
lines = []
lines.append("# Chaos Report\n")
lines.append(f"- **Artifact dir:** `{art}`")
if producer_summary:
    lines.append(f"- **Producer log:** `{producer_summary.get('file')}`")
lines.append("")

if summary["anomalies"]:
    lines.append("## Quick Findings")
    for a in summary["anomalies"]:
        lines.append(f"- {a}")
    lines.append("")

if producer_summary:
    lines.append("## Producer Summary")
    lines.append(f"- Total lines: **{producer_summary['total_lines']}**")
    lines.append(f"- ok=true: **{producer_summary['ok_true']}**")
    lines.append(f"- ok=false: **{producer_summary['ok_false']}**")
    lines.append(f"- JSON/line parse errors: **{producer_summary['json_parse_errors']}**")
    if producer_summary["http_code_counts"]:
        sc = ", ".join(f"{k}={v}" for k, v in producer_summary["http_code_counts"].items())
        lines.append(f"- HTTP code counts: {sc}")
    if producer_summary["target_port_counts"]:
        pc = ", ".join(f"{k}={v}" for k, v in producer_summary["target_port_counts"].items())
        lines.append(f"- Target port counts: {pc}")
    lines.append("")

for phase, p in summary["phases"].items():
    lines.append(f"## Phase: `{phase}`")
    leaders = p["leaders_reported_by_self"]
    if leaders:
        lines.append("- Self-reported leader(s): " + ", ".join(f"{x['node_id']}@{x['port']}" for x in leaders))
    else:
        lines.append("- Self-reported leader(s): none")
    if p["followers_with_zero_heartbeats_received"]:
        lines.append("- Followers with zero heartbeats received: " +
                     ", ".join(f"{x['node_id']}@{x['port']}" for x in p["followers_with_zero_heartbeats_received"]))
    lines.append("")
    lines.append("| Port | Node | Role | State | Term | Elections | HB Sent | HB Recv | Commit | Applied | Repl OK | Repl Fail |")
    lines.append("|---:|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|")
    for port, row in sorted(p["ports"].items(), key=lambda kv: int(kv[0]) if str(kv[0]).isdigit() else 99999):
        lines.append(
            f"| {port} | {row['self_node_id'] or '-'} | {row['role']} | {row['state']} | "
            f"{row['term'] if row['term'] is not None else '-'} | "
            f"{row['elections_total'] if row['elections_total'] is not None else '-'} | "
            f"{row['heartbeats_sent_total'] if row['heartbeats_sent_total'] is not None else '-'} | "
            f"{row['heartbeats_received_total'] if row['heartbeats_received_total'] is not None else '-'} | "
            f"{row['commit_index'] if row['commit_index'] is not None else '-'} | "
            f"{row['applied_index'] if row['applied_index'] is not None else '-'} | "
            f"{row['writes_replicated_total'] if row['writes_replicated_total'] is not None else '-'} | "
            f"{row['writes_failed_total'] if row['writes_failed_total'] is not None else '-'} |"
        )
    lines.append("")

    # Topic/partition files present for this phase
    if phase in summary["topic_partition_files_by_phase"]:
        lines.append(f"### Topic/Partition snapshots in `{phase}`")
        for port, files in sorted(summary["topic_partition_files_by_phase"][phase].items(), key=lambda kv: int(kv[0])):
            lines.append(f"- **port {port}:** " + ", ".join(f"`{f}`" for f in files))
        lines.append("")

if interesting_files:
    lines.append("## Topic/Partition Related Files (index)")
    for rel in interesting_files:
        lines.append(f"- `{rel}`")
    lines.append("")

# Write outputs
json_path = summary_dir / "chaos_report.json"
md_path = summary_dir / "chaos_report.md"
json_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

print(f"Wrote: {md_path}")
print(f"Wrote: {json_path}")
PY