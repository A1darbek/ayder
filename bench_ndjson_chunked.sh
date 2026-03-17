#!/usr/bin/env bash
set -euo pipefail
LC_ALL=C

# ── config via env ────────────────────────────────────────────────────────────
BASE="${BASE:-http://127.0.0.1:1109}"
TOKEN="${TOKEN:-dev}"
TOPIC="${TOPIC:-orders}"
PART="${PART:-0}"
STREAMS="${STREAMS:-8}"          # concurrent producers
LINES="${LINES:-1000}"           # NDJSON lines per stream
PAD="${PAD:-64}"                 # pad string length per record
SLEEP_MS="${SLEEP_MS:-0}"        # per-line sleep (ms); 0 = blast
EXTRA_Q="${EXTRA_Q:-}"           # e.g. "timeout_ms=200"
ENSURE_TOPIC="${ENSURE_TOPIC:-1}"

need() { command -v "$1" >/dev/null 2>&1 || { echo "missing: $1" >&2; exit 1; }; }
need curl
need awk
need date
need tr
need head

# ── tiny helpers ──────────────────────────────────────────────────────────────
randhex(){ hexdump -vn8 -e '8/1 "%02x"' /dev/urandom; }
mk_pad(){ head -c "$PAD" < /dev/zero | tr '\0' 'a'; }
now_ms(){ date +%s%3N 2>/dev/null || awk 'BEGIN{srand(); print int(systime()*1000)}'; }
ms_sleep(){ [ "$1" -gt 0 ] && awk -v m="$1" 'BEGIN{printf "%.6f", m/1000}' | { read s; sleep "$s"; }; }

pct() { # pct 0.90 <sorted numbers...>
  local p="$1"; shift; local n="$#"; [ "$n" -eq 0 ] && { echo 0; return; }
  local idx; idx=$(awk -v n="$n" -v p="$p" 'BEGIN{printf "%d",(n-1)*p}')
  shift $idx 2>/dev/null || true; echo "${1:-0}"
}

build_path(){
  local idk="$1"
  local p="/broker/topics/${TOPIC}/produce-ndjson?partition=${PART}&idempotency_key=${idk}"
  [ -n "$EXTRA_Q" ] && p="${p}&${EXTRA_Q}"
  printf '%s' "$p"
}

# ── ensure topic (optional) ───────────────────────────────────────────────────
if [ "$ENSURE_TOPIC" = "1" ]; then
  curl -sS -X POST "$BASE/broker/topics" \
    -H 'Content-Type: application/json' \
    -H "Authorization: Bearer ${TOKEN}" \
    -d "{\"name\":\"$TOPIC\",\"partitions\":8}" >/dev/null || true
fi

# ── one stream (chunked) via curl ─────────────────────────────────────────────
# We rely on: HTTP/1.1 + stdin + unknown length  => curl uses chunked.
stream_one() {
  local idx="$1" tmp="$2"
  local idk="curl-${idx}-$(randhex)"
  local url="${BASE}$(build_path "$idk")"
  local pad; pad="$(mk_pad)"
  local body_pipe; body_pipe="$(mktemp -u)"
  mkfifo "$body_pipe"

  # producer of NDJSON lines → pipe
  (
    for ((i=1;i<=LINES;i++)); do
      printf '{"id":%d,"name":"user-%d","score":%d,"pad":"%s"}\n' "$i" "$i" $((i%100)) "$pad"
      ms_sleep "$SLEEP_MS"
    done
  ) > "$body_pipe" &

  local start end dur code outfile="$tmp/resp_${idx}.json"
  start="$(now_ms)"
  # -H 'Expect:' disables 100-continue delay; --http1.1 keeps it on 1.1.
  code=$(
    curl --http1.1 -sS -X POST "$url" \
      -H 'Content-Type: application/x-ndjson' \
      -H "Authorization: Bearer ${TOKEN}" \
      -H 'Expect:' \
      --no-buffer \
      --data-binary @- \
      -o "$outfile" \
      -w '%{http_code}' < "$body_pipe"
  )
  end="$(now_ms)"; dur=$(( end - start ))
  rm -f "$body_pipe"
  printf 'dur_ms=%s status=%s out=%s\n' "$dur" "$code" "$outfile" > "$tmp/stream_${idx}.meta"
}

# ── run ───────────────────────────────────────────────────────────────────────
echo "→ chunked NDJSON via curl: $BASE  topic=$TOPIC part=$PART"
echo "  streams=$STREAMS  lines/stream=$LINES  pad=$PAD  sleep_ms=$SLEEP_MS"

tmpdir="$(mktemp -d)"; trap 'rm -rf "$tmpdir"' EXIT
t0="$(now_ms)"
pids=()
for ((s=1;s<=STREAMS;s++)); do stream_one "$s" "$tmpdir" & pids+=($!); done
for p in "${pids[@]}"; do wait "$p" || true; done
t1="$(now_ms)"

# ── aggregate ─────────────────────────────────────────────────────────────────
oks=0; errs=0; durs=()
for meta in "$tmpdir"/stream_*.meta; do
  [ -e "$meta" ] || continue
  # shellcheck disable=SC1090
  . "$meta"
  if [ "${status:-0}" -ge 200 ] && [ "${status:-0}" -lt 300 ]; then
    oks=$((oks+1)); durs+=("${dur_ms}")
  else
    errs=$((errs+1))
    echo "stream error: status=${status:-0} dur_ms=${dur_ms:-?} file=${out:-?}" >&2
    [ -n "${out:-}" ] && head -c 256 "$out" 2>/dev/null >&2 || true
  fi
done

IFS=$'\n' sorted=($(printf '%s\n' "${durs[@]}" | sort -n)); unset IFS
p50="$(pct 0.50 "${sorted[@]}")"
p90="$(pct 0.90 "${sorted[@]}")"
p99="$(pct 0.99 "${sorted[@]}")"

elapsed=$(( t1 - t0 ))
total_lines=$(( STREAMS * LINES ))
lps=0; [ "$elapsed" -gt 0 ] && lps=$(( total_lines * 1000 / elapsed ))

echo
echo "===== CHUNKED NDJSON (curl) SUMMARY ====="
echo "streams:   ${oks} ok / ${errs} err"
echo "lines:     ${total_lines} (per stream=${LINES})"
echo "elapsed:   ${elapsed} ms (wall)"
echo "throughput ~${lps} lines/sec"
[ "$oks" -gt 0 ] && {
  echo "latency per stream (req start → resp end):"
  echo "  p50: ${p50} ms"
  echo "  p90: ${p90} ms"
  echo "  p99: ${p99} ms"
}
echo "========================================="
