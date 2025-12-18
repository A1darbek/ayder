-- NDJSON batch producer for /broker/topics/:topic/produce-ndjson
-- Sends generic JSON lines (and optionally mixed types) with per-request batches.
-- Uses scripts/_auth just like your single-produce script.

wrk.method = "POST"
wrk.headers["Content-Type"] = "application/x-ndjson"
wrk.headers["Transfer-Encoding"] = "chunked"

local auth = require "scripts/_auth"

-- ── env knobs ────────────────────────────────────────────────────────────────
local topic        = os.getenv("TOPIC") or "orders"
local part         = os.getenv("PART")         -- optional
local batch_lines  = tonumber(os.getenv("BATCH") or "16")      -- lines per request
local paylen       = tonumber(os.getenv("PAYLOAD") or "64")    -- bytes in .pad
local mix          = tonumber(os.getenv("MIX") or "0")         -- 0=objects only; 1=mix in strings
local blank_every  = tonumber(os.getenv("BLANK_EVERY") or "0") -- 0=never; N=every Nth line insert blank
local idem         = tonumber(os.getenv("IDEM") or "0")        -- 1=attach idempotency_key per request

-- Precompute a few payload “pads” to vary bytes a bit
local PAD_COUNT = 64
local pads = {}
for i = 1, PAD_COUNT do
  local s = {}
  for j = 1, paylen do
    -- simple deterministic bytes without allocating too much CPU
    s[j] = string.char(((i + j) % 26) + 97)  -- letters a..z
  end
  pads[i] = table.concat(s)
end

-- Small helpers
local function urlenc(s)
  return (s:gsub("([^%w%-_%.~])", function(c)
    return string.format("%%%02X", string.byte(c))
  end))
end

-- per-thread state
local tid = "t0"
local seq = 0
local pad_idx = 0

function setup(thread)
  auth.setup(thread)
  -- stash a per-thread id we can use for idempotency keys
  thread:set("tid", tostring(thread))
end

function init(args)
  auth.init()
  tid = wrk.thread:get("tid") or "t0"
  local seed = os.time()
  local hex = tonumber((tid:match("0x([0-9a-fA-F]+)")) or "0", 16) or 0
  math.randomseed(seed + hex)
  pad_idx = math.random(1, PAD_COUNT)
end

local function next_pad()
  pad_idx = pad_idx + 1
  if pad_idx > PAD_COUNT then pad_idx = 1 end
  return pads[pad_idx]
end

local function make_line_object(id)
  -- keep it generic; no key/value schema
  -- e.g. {"id":123,"name":"user-123","score":87,"pad":"..."}
  local score = (id * 37) % 100
  local pad = next_pad()
  -- Manual string build is faster than JSON libs and fine for controlled fields
  return string.format("{\"id\":%d,\"name\":\"user-%d\",\"score\":%d,\"pad\":\"%s\"}", id, id, score, pad)
end

local function make_line_string()
  return string.format("\"%s\"", next_pad())
end

function request()
  seq = seq + 1

  -- Build NDJSON body: batch_lines lines, each ending with \n
  local lines = {}
  local n = 0
  for i = 1, batch_lines do
    -- Optional blank line injection (tests server’s blank-line handling)
    if blank_every > 0 and (i % blank_every == 0) then
      n = n + 1
      lines[n] = ""  -- blank line
    else
      if mix == 1 and (i % 8 == 0) then
        n = n + 1
        lines[n] = make_line_string()
      else
        n = n + 1
        lines[n] = make_line_object(seq * 100000 + i) -- stable-ish unique id space
      end
    end
  end
  lines[n + 1] = ""  -- ensure trailing newline via table.concat with "\n"
  local body = table.concat(lines, "\n")

  -- Build path
  local q = {}
  if part and #part > 0 then q[#q+1] = "partition=" .. urlenc(part) end
  if idem == 1 then
    -- unique enough idempotency key per request
    local idk = string.format("wrk-ndjson-%s-%d", tid, seq)
    q[#q+1] = "idempotency_key=" .. urlenc(idk)
  end
  local path = string.format("/broker/topics/%s/produce-ndjson", topic)
  if #q > 0 then path = path .. "?" .. table.concat(q, "&") end

  return wrk.format(nil, path, nil, body)
end
