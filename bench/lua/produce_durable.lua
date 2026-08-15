local threads = {}

local topic = os.getenv("TOPIC") or "rrv1"
local partition = os.getenv("PARTITION") or "0"
local payload_bytes = tonumber(os.getenv("PAYLOAD_BYTES") or "128")
local timeout_ms = os.getenv("TIMEOUT_MS") or "5000"
local token = os.getenv("TOKEN") or "dev"

local payload = string.rep("x", payload_bytes)
local path = string.format(
  "/broker/topics/%s/produce?partition=%s&timeout_ms=%s",
  topic,
  partition,
  timeout_ms
)

local ok_count = 0
local invalid_count = 0
local status_2xx = 0
local status_4xx = 0
local status_429 = 0
local status_5xx = 0

_G.ayder_ok_count = 0
_G.ayder_invalid_count = 0
_G.ayder_status_2xx = 0
_G.ayder_status_4xx = 0
_G.ayder_status_429 = 0
_G.ayder_status_5xx = 0

function setup(thread)
  table.insert(threads, thread)
end

function init(args)
  wrk.method = "POST"
  wrk.body = payload
  wrk.headers["Content-Type"] = "application/octet-stream"
  if token ~= "" then
    wrk.headers["Authorization"] = "Bearer " .. token
  end
end

function request()
  return wrk.format(nil, path)
end

function response(status, headers, body)
  if status >= 200 and status < 300 then
    status_2xx = status_2xx + 1
    if body and body:match('"ok"%s*:%s*true')
       and body:match('"durability_scope"%s*:%s*"local"')
       and body:match('"local_fsync"%s*:%s*true')
       and body:match('"quorum_committed"%s*:%s*false')
       and body:match('"failover_safe"%s*:%s*false')
       and body:match('"applied_all_workers"%s*:%s*true')
       and body:match('"durable"%s*:%s*true')
       and body:match('"synced"%s*:%s*true') then
      ok_count = ok_count + 1
    else
      invalid_count = invalid_count + 1
    end
  elseif status == 429 then
    status_4xx = status_4xx + 1
    status_429 = status_429 + 1
  elseif status >= 400 and status < 500 then
    status_4xx = status_4xx + 1
  elseif status >= 500 then
    status_5xx = status_5xx + 1
  else
    invalid_count = invalid_count + 1
  end

  _G.ayder_ok_count = ok_count
  _G.ayder_invalid_count = invalid_count
  _G.ayder_status_2xx = status_2xx
  _G.ayder_status_4xx = status_4xx
  _G.ayder_status_429 = status_429
  _G.ayder_status_5xx = status_5xx
end

function done(summary, latency, requests)
  local errors = summary.errors or {}
  local totals = {
    ok = 0,
    invalid = 0,
    status_2xx = 0,
    status_4xx = 0,
    status_429 = 0,
    status_5xx = 0,
  }

  for _, thread in ipairs(threads) do
    totals.ok = totals.ok + (thread:get("ayder_ok_count") or 0)
    totals.invalid = totals.invalid + (thread:get("ayder_invalid_count") or 0)
    totals.status_2xx = totals.status_2xx + (thread:get("ayder_status_2xx") or 0)
    totals.status_4xx = totals.status_4xx + (thread:get("ayder_status_4xx") or 0)
    totals.status_429 = totals.status_429 + (thread:get("ayder_status_429") or 0)
    totals.status_5xx = totals.status_5xx + (thread:get("ayder_status_5xx") or 0)
  end

  io.write(string.format(
    "AYDER_VALIDATION requests=%d ok=%d invalid=%d status_2xx=%d status_4xx=%d status_429=%d status_5xx=%d connect_errors=%d read_errors=%d write_errors=%d timeout_errors=%d\n",
    summary.requests,
    totals.ok,
    totals.invalid,
    totals.status_2xx,
    totals.status_4xx,
    totals.status_429,
    totals.status_5xx,
    errors.connect or 0,
    errors.read or 0,
    errors.write or 0,
    errors.timeout or 0
  ))
  io.write(string.format(
    "AYDER_LATENCY_US p50=%.0f p95=%.0f p99=%.0f p99_9=%.0f max=%.0f\n",
    latency:percentile(50.0),
    latency:percentile(95.0),
    latency:percentile(99.0),
    latency:percentile(99.9),
    latency.max
  ))
end
