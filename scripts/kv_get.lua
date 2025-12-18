wrk.method = "GET"
local ns = "global"
function request()
  local id = math.random(1, 100000000)
  local key = string.format("k%08d", id)
  local path = string.format("/v1/kv/%s/%s", ns, key)
  return wrk.format("GET", path)
end
