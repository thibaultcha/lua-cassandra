local _M = {}

function _M.new_policy(name)
  local _reconn_mt = {
    name = name,
    reset = function() error('reset() not implemented') end,
    next_delay = function() error('next_delay() not implemented') end,
  }

  _reconn_mt.__index = _reconn_mt

  _reconn_mt.super = {
    new = function()
      return setmetatable({}, _reconn_mt)
    end
  }

  return setmetatable(_reconn_mt, {
    __index = _reconn_mt.super
  })
end

return _M
