-- Consumer for /broker/consume/:topic/:group/:partition
-- env: TOPIC (default "orders"), GROUP (default "bench"), PART (default 0), LIMIT (default 100)
wrk.method = "GET"

local topic     = os.getenv("TOPIC") or "orders"
local group     = os.getenv("GROUP") or "bench"
local partition = tonumber(os.getenv("PART") or "0")
local limit     = tonumber(os.getenv("LIMIT") or "500")

local offset = -1 -- exclusive: let server clamp to earliest available

function request()
local path = string.format("/broker/consume/%s/%s/%d?limit=%d&offset=%d",
                             topic, group, partition, limit, offset)
  return wrk.format(nil, path)
end

function response(status, headers, body)
  -- best-effort parse of next_offset from JSON
  local no = body and string.match(body, '"next_offset"%s*:%s*(%d+)')
  if no then offset = tonumber(no) end
end
