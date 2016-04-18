local _M = {}

function _M.new_policy(name)
  local _lb_mt = {
    name = name,
    init = function() error('init() not implemented') end,
    next_peer = function() error('next_peer() not implemented') end,
  }

  _lb_mt.__index = _lb_mt

  _lb_mt.super = {
    new = function()
      return setmetatable({}, _lb_mt)
    end
  }

  return setmetatable(_lb_mt, {
    __index = _lb_mt.super
  })
end

return _M
