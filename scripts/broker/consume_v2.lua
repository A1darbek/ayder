wrk.method = "GET"
local auth = require "scripts/_auth"

local topic     = os.getenv("TOPIC") or "orders"
local group     = os.getenv("GROUP") or "bench"
local partition = tonumber(os.getenv("PART") or "0")
local limit     = tonumber(os.getenv("LIMIT") or "500")
local want_b64  = (os.getenv("B64") == "1")

local offset = -1  -- from_exclusive

function setup(thread) auth.setup(thread) end
function init(args)    auth.init() end

function request()
  local path
  if want_b64 then
    path = string.format("/broker/consume/%s/%s/%d?limit=%d&offset=%d&encoding=b64",
                         topic, group, partition, limit, offset)
  else
    path = string.format("/broker/consume/%s/%s/%d?limit=%d&offset=%d",
                         topic, group, partition, limit, offset)
  end
  return wrk.format(nil, path)
end

function response(status, headers, body)
  local no = body and string.match(body, '"next_offset"%s*:%s*(%d+)')
  if no then offset = tonumber(no) end
end
