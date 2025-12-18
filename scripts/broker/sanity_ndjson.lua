-- Sends a tiny valid NDJSON POST with a Content-Length.
local topic  = os.getenv("TOPIC") or "orders"
local part   = os.getenv("PART") or "0"
local token  = (os.getenv("TOKENS") or os.getenv("TOKEN") or "dev"):gsub(";", ","):match("([^,]+)") or "dev"

wrk.method  = "POST"
wrk.headers = {
  ["Authorization"] = "Bearer " .. token,
  ["Content-Type"]  = "application/x-ndjson"
}

local path = string.format("/broker/topics/%s/produce-ndjson?partition=%s", topic, part)
local body = "xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n" -- 1 raw line

request = function()
  return wrk.format(nil, path, nil, body) -- wrk adds Content-Length
end
