-- post.lua  ─ hammer POST /users with unique JSON bodies
-- usage: wrk -t4 -c200 -d30s -s post.lua http://127.0.0.1:1109/users

wrk.method  = "POST"
wrk.headers["Content-Type"] = "application/json"

local counter = 0      -- one counter *per Lua VM* (per wrk thread)
local id_base = 0      -- unique offset so IDs don’t clash

init = function(args)
  -- Newer wrk gives you `thread.id`; older ones don’t.  Fall back to 0.
  id_base = (thread and thread["id"] or 0) * 1000000000
end

request = function()
  counter = counter + 1
  local id   = id_base + counter
  local body = string.format('{"id":%d,"name":"user%d"}', id, id)
  return wrk.format(nil, nil, nil, body)
end

