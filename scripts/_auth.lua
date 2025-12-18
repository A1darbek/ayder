local M = {}

local function split_csv(s)
  local t = {}
  for part in string.gmatch(s or "", "([^,;]+)") do
    part = part:gsub("^%s+", ""):gsub("%s+$", "")
    if #part > 0 then table.insert(t, part) end
  end
  return t
end

function M.setup(thread)
  local pool = split_csv(os.getenv("TOKENS"))
  if #pool == 0 then
    local single = os.getenv("TOKEN")
    if single and #single > 0 then pool = { single } end
  end
  local token = nil
  if #pool > 0 then
    M._tid = (M._tid or 0) + 1
    token = pool[((M._tid - 1) % #pool) + 1]
  end
  thread:set("auth_token", token or "")
end

function M.init()
  local tok = wrk.thread:get("auth_token") or ""
  if #tok > 0 then wrk.headers["Authorization"] = "Bearer " .. tok end
end

return M
