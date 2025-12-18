local s = 0xdeadcafe
function rand_u32()
  s = (s * 1103515245 + 12345) & 0xffffffff
  return s
end

request = function()
  -- hot set the same size as your torture WRITE_KEYS
  local id = rand_u32() % 200000 + 1
  return wrk.format("GET", "/users/" .. id)
end