-- ============================================================================
-- DEFERRED INITIALIZATION: wrk global only available in callbacks for giltene/wrk2
-- ============================================================================
local method_set = false
local auth = require "scripts/_auth"

local topic   = os.getenv("TOPIC") or "orders"
local part    = os.getenv("PART")
local paylen  = tonumber(os.getenv("PAYLOAD") or "64")

-- enable timing only when TIMING=1
local want_timing = (os.getenv("TIMING") == "1")
local timing_q = want_timing and "&timing=1" or ""

-- keep TOP-K largest values per thread for p99.999 tail approximation
local TOPK = tonumber(os.getenv("SERVER_TOPK") or "256")

-- payload pool
local payloads = {}
local N = 64
for i = 1, N do
  local s = {}
  for j = 1, paylen do s[j] = string.char((i + j) % 256) end
  payloads[i] = table.concat(s)
end

local seq, idx = 0, 0

-- ============================================================================
-- TOPK collector helper (keeps TOPK largest values, exports CSV for merging)
-- ============================================================================
local function new_topk(name)
  local o = {}
  o.name = name
  o.n = 0
  o.max = 0

  o.top = {}
  o.top_n = 0
  o.top_min = 0
  o.top_min_i = 1

  -- exported field name: e.g. server_top_csv, queue_top_csv, recv_top_csv
  o.csv_key = name .. "_top_csv"
  _G[o.csv_key] = ""     -- EXPORT string

  -- exported scalar keys: e.g. server_n, server_max
  o.n_key   = name .. "_n"
  o.max_key = name .. "_max"
  _G[o.n_key] = 0
  _G[o.max_key] = 0

  local function recompute_min()
    local mn = o.top[1]
    local mi = 1
    for i=2, o.top_n do
      local v = o.top[i]
      if v < mn then mn = v; mi = i end
    end
    o.top_min = mn
    o.top_min_i = mi
  end

  local function export_csv()
    if o.top_n == 0 then
      _G[o.csv_key] = ""
    else
      _G[o.csv_key] = table.concat(o.top, ",", 1, o.top_n)
    end
  end

  function o:push(v)
    if not v then return end
    self.n = self.n + 1
    _G[self.n_key] = self.n

    if v > self.max then
      self.max = v
      _G[self.max_key] = self.max
    end

    if self.top_n < TOPK then
      self.top_n = self.top_n + 1
      self.top[self.top_n] = v
      if self.top_n == 1 then
        self.top_min = v; self.top_min_i = 1
      elseif v < self.top_min then
        self.top_min = v; self.top_min_i = self.top_n
      end
      export_csv()
      return
    end

    if v > self.top_min then
      self.top[self.top_min_i] = v
      recompute_min()
      export_csv()
    end
  end

  return o
end

-- collectors
local C_server = new_topk("server")       -- parses "server_us"
local C_queue  = new_topk("queue")        -- parses "queue_us"
local C_recv   = new_topk("recv_parse")   -- parses "recv_parse_us"

local function urlenc(s)
  return (s:gsub("([^%w%-_%.~])", function(c)
    return string.format("%%%02X", string.byte(c))
  end))
end

-- collect thread handles so done() can pull per-thread exported values
local threads = {}
function setup(thread)
  table.insert(threads, thread)
  auth.setup(thread)
end

function init(args)
  -- NOW wrk global is available - set method and headers here
  if not method_set then
    wrk.method = "POST"
    wrk.headers["Content-Type"] = "application/octet-stream"
    method_set = true
  end
  auth.init()
end

function request()
  seq = seq + 1
  idx = (idx % N) + 1
  local key = "k-" .. seq
  local path
  if part then
    path = string.format("/broker/topics/%s/produce?partition=%s&key=%s%s",
      topic, part, urlenc(key), timing_q)
  else
    path = string.format("/broker/topics/%s/produce?key=%s%s",
      topic, urlenc(key), timing_q)
  end
  return wrk.format(nil, path, nil, payloads[idx])
end

function response(status, headers, body)
  if not want_timing then return end
  if not body then return end

  -- NOTE: these must match your JSON keys exactly:
  -- "server_us", "queue_us", "recv_parse_us"
  local s_server = body:match('"server_us"%s*:%s*(%d+)')
  if s_server then C_server:push(tonumber(s_server)) end

  local s_queue  = body:match('"queue_us"%s*:%s*(%d+)')
  if s_queue then C_queue:push(tonumber(s_queue)) end

  local s_recv   = body:match('"recv_parse_us"%s*:%s*(%d+)')
  if s_recv then C_recv:push(tonumber(s_recv)) end
end

local function fmt_us(us)
  if not us then return "n/a" end
  return string.format("%dus (%.3fms)", us, us/1000.0)
end

-- Merge helper: read per-thread n/max/csv for a given collector name
local function merge_metric(name)
  local n_key   = name .. "_n"
  local max_key = name .. "_max"
  local csv_key = name .. "_top_csv"

  local total_n = 0
  local merged = {}
  local merged_max = 0

  for _, t in ipairs(threads) do
    local n  = t:get(n_key) or 0
    local mx = t:get(max_key) or 0
    local csv = t:get(csv_key) or ""

    total_n = total_n + n
    if mx > merged_max then merged_max = mx end

    for num in csv:gmatch("(%d+)") do
      merged[#merged+1] = tonumber(num)
    end
  end

  return total_n, merged, merged_max
end

local function p99999_from_tail(total_n, merged_desc_sorted)
  -- p99.999 ≈ (tail+1)-th largest, where tail=floor(n*(1-q))
  local q = 0.99999
  local tail = math.floor(total_n * (1.0 - q))
  local i = tail + 1
  if i < 1 then i = 1 end
  if i > #merged_desc_sorted then i = #merged_desc_sorted end
  return merged_desc_sorted[i], tail
end

function done(summary, latency, requests)
  -- CLIENT line
  local client_p99999 = latency:percentile(99.999)
  local client_max = latency.max
  io.write(string.format("CLIENT  p99.999=%s  max=%s\n",
    fmt_us(client_p99999), fmt_us(client_max)))

  if not want_timing then
    io.write("SERVER  timing disabled (set TIMING=1)\n")
    return
  end

  -- Merge + print each metric
  local function print_metric(label, name)
    local total_n, merged, mx = merge_metric(name)
    if total_n == 0 or #merged == 0 then
      io.write(string.format("SERVER  %s no samples collected\n", label))
      return
    end

    table.sort(merged, function(a,b) return a > b end) -- desc
    local p, tail = p99999_from_tail(total_n, merged)

    io.write(string.format(
      "SERVER  %s p99.999=%s  max=%s  (n=%d, merged=%d, TOPK/thread=%d)\n",
      label, fmt_us(p), fmt_us(mx), total_n, #merged, TOPK
    ))
  end

  print_metric("server_us",      "server")
  print_metric("queue_us",       "queue")
  print_metric("recv_parse_us",  "recv_parse")
end