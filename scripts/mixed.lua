-- Mixed GET/POST with hot-spot skew. Designed for your handlers:
--  POST /users {id, name}
--  GET  /users/:id

-- ENV knobs:
--   RF_WRITE_KEYS   total keyspace (default 200000)
--   RF_HOT_KEYS     hot subset size (default 1% of keyspace)
--   RF_WRITE_PCT    % of requests that are writes (default 20)
--   RF_ZIPF_S       zipf-ish exponent (default 1.1)

math.randomseed(os.time())

local WRITE_KEYS = tonumber(os.getenv("RF_WRITE_KEYS") or "200000")
local HOT_KEYS   = tonumber(os.getenv("RF_HOT_KEYS") or math.max(1, math.floor(WRITE_KEYS * 0.01)))
local WRITE_PCT  = tonumber(os.getenv("RF_WRITE_PCT") or "20")
local ZIPF_S     = tonumber(os.getenv("RF_ZIPF_S") or "1.1")

-- Precompute a hot-set key list with approximate Zipf selection.
local hot = {}
for i = 1, HOT_KEYS do hot[i] = i end

local function zipf_hot()
  -- inverse CDF sampler for a coarse zipf-ish distribution on [1..HOT_KEYS]
  local u = math.random()
  local x = math.floor(1 / (u ^ (1 / (ZIPF_S))) )
  if x < 1 then x = 1 end
  if x > HOT_KEYS then x = HOT_KEYS end
  return hot[x]
end

local function pick_id()
  if math.random(100) <= 80 then
    -- 80% from hot set, 20% from cold set
    return zipf_hot()
  else
    return math.random(HOT_KEYS + 1, WRITE_KEYS)
  end
end

request = function()
  local id = pick_id()
  if math.random(100) <= WRITE_PCT then
    -- POST (update name quickly so AOF has churn)
    local name = "user_" .. id .. "_" .. tostring(math.random(1e9))
    local body = string.format('{"id":%d,"name":"%s"}', id, name)
    wrk.method = "POST"
    wrk.body   = body
    wrk.headers["Content-Type"] = "application/json"
    return wrk.format(nil, "/users")
  else
    -- GET
    wrk.method = "GET"
    wrk.body   = nil
    return wrk.format(nil, "/users/" .. id)
  end
end
