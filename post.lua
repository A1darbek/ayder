wrk.method = "POST"
wrk.headers["Content-Type"] = "application/json"

counter = 0
local threads = {}

-- Setup metrics per thread
function setup(thread)
  thread:set("bb", 0) -- body bytes
  thread:set("s2", 0); thread:set("s3", 0)
  thread:set("s4", 0); thread:set("s5", 0); thread:set("so", 0)
  table.insert(threads, thread)
end

-- Send POST to /users (not /users/:id)
function request()
  counter = counter + 1
  local id = counter
  local body = string.format('{"id":%d,"name":"t%06d"}', id, id)
  return wrk.format(nil, "/users",
    { ["Content-Type"] = "application/json", ["Content-Length"] = #body }, body)
end

-- Collect response stats
function response(s, h, b)
  local t = wrk.thread
  local bb = t:get("bb") or 0
  local cl = tonumber(h["content-length"])
  if cl then bb = bb + cl elseif b then bb = bb + #b end
  t:set("bb", bb)

  if     s >= 200 and s < 300 then t:set("s2", (t:get("s2") or 0) + 1)
  elseif s >= 300 and s < 400 then t:set("s3", (t:get("s3") or 0) + 1)
  elseif s >= 400 and s < 500 then t:set("s4", (t:get("s4") or 0) + 1)
  elseif s >= 500 and s < 600 then t:set("s5", (t:get("s5") or 0) + 1)
  else                            t:set("so", (t:get("so") or 0) + 1)
  end
end

-- Print final stats
function done(summary, latency, requests)
  local bb,s2,s3,s4,s5,so = 0,0,0,0,0,0
  for _,t in ipairs(threads) do
    bb = bb + (t:get("bb") or 0)
    s2 = s2 + (t:get("s2") or 0)
    s3 = s3 + (t:get("s3") or 0)
    s4 = s4 + (t:get("s4") or 0)
    s5 = s5 + (t:get("s5") or 0)
    so = so + (t:get("so") or 0)
  end
  local total = summary.bytes
  print(string.format(
    "reqs=%d  total=%dB  body=%dB  avg_total=%.1fB  avg_body=%.1fB  avg_hdr~=%.1fB",
    summary.requests, total, bb,
    total/summary.requests, bb/summary.requests, (total-bb)/summary.requests))
  print(string.format("2xx=%d 3xx=%d 4xx=%d 5xx=%d other=%d", s2,s3,s4,s5,so))
end
