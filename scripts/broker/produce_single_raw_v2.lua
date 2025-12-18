wrk.method = "POST"
wrk.headers["Content-Type"] = "application/octet-stream"
local auth = require "scripts/_auth"

local topic   = os.getenv("TOPIC") or "orders"
local part    = os.getenv("PART")
local paylen  = tonumber(os.getenv("PAYLOAD") or "64")

local payloads = {}
local N = 64
for i = 1, N do
  local s = {}
  for j = 1, paylen do s[j] = string.char((i + j) % 256) end
  payloads[i] = table.concat(s)
end

local seq, idx = 0, 0

local function urlenc(s) return (s:gsub("([^%w%-_%.~])", function(c) return string.format("%%%02X", string.byte(c)) end)) end

function setup(thread) auth.setup(thread) end
function init(args)    auth.init() end

function request()
  seq = seq + 1
  idx = (idx % N) + 1
  local key = "k-" .. seq
  local path
  if part then
    path = string.format("/broker/topics/%s/produce?partition=%s&key=%s", topic, part, urlenc(key))
  else
    path = string.format("/broker/topics/%s/produce?key=%s", topic, urlenc(key))
  end
  return wrk.format(nil, path, nil, payloads[idx])
end
