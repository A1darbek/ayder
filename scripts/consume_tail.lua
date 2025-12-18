-- GET /broker/consume/:topic/:group/:part?limit=N&offset=O, tracking offset per thread
function setup(thread)
  thread:set("topic",  os.getenv("TOPIC")  or "orders")
  thread:set("group",  os.getenv("GROUP")  or "bench-batch")
  thread:set("part",   os.getenv("PART")   or "0")
  thread:set("limit",  os.getenv("LIMIT")  or "500")
  thread:set("offset", 0x7fffffffffffffff)
end

function request()
  local path = string.format("/broker/consume/%s/%s/%s?limit=%s&offset=%s",
                             topic, group, part, limit, tostring(offset))
  return wrk.format("GET", path)
end

function response(status, headers, body)
  -- cheap parse for next_offset
  local noff = string.match(body, '"next_offset"%s*:%s*(%d+)')
  if noff then offset = tonumber(noff) end
end
