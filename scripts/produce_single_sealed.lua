-- Single-message producer for /broker/produce
-- env: TOPIC (default "orders"), PART (optional fixed partition)
wrk.method = "POST"
wrk.headers["Content-Type"] = "application/json"

local topic = os.getenv("TOPIC") or "orders"
local part  = os.getenv("PART")   -- e.g. "0"
local seq   = 0                   -- per-thread counter (each wrk thread has its own Lua VM)

function request()
  seq = seq + 1
  local key = "k-" .. seq
  local val = "v-" .. seq
  local body
  if part then
    body = string.format('{"topic":"%s","key":"%s","value":"%s","partition":%s}', topic, key, val, part)
  else
    body = string.format('{"topic":"%s","key":"%s","value":"%s"}', topic, key, val)
  end
  return wrk.format(nil, "/broker/produce-sealed", nil, body)
end
