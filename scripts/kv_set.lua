wrk.method = "POST"
local ns = "global"
function request()
  local id = math.random(1, 100000000)
  local key = string.format("k%08d", id)
  local body = string.format('{"value":"v%08d"}', id)
  local path = string.format("/v1/kv/%s/%s", ns, key)
  return wrk.format("POST", path, {["Content-Type"]="application/json"}, body)
end
