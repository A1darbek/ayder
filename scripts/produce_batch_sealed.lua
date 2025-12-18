local threads = {}

function setup(thread)
  local topic = os.getenv("TOPIC") or "orders"
  local part  = os.getenv("PART")  or "0"
  local batch = tonumber(os.getenv("BATCH") or "32")

  local bodies = {}
  for k = 1, 64 do
    local msgs = {}
    for j = 1, batch do
      -- tiny payloads keep you in the 256B inline fast-path
      msgs[#msgs+1] = string.format('{"key":"k%d-%d","value":"v%d-%d"}', k, j, k, j)
    end
    bodies[k] = string.format('{"topic":"%s","messages":[%s],"partition":%s}',
                              topic, table.concat(msgs, ","), part)
  end
  thread:set("bodies", bodies)
  thread:set("idx", 0)
end

function init(args)
  wrk.method = "POST"
  wrk.headers["Content-Type"] = "application/json"
end

function request()
  idx = (idx % #bodies) + 1
  return wrk.format(nil, "/broker/produce-batch-sealed", nil, bodies[idx])
end