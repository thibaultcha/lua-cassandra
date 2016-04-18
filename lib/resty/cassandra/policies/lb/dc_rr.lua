local _M = require('resty.cassandra.policies.lb').new_policy('dc_aware_round_robin')

function _M.new(local_dc)
  local self = _M.super.new()
  self.local_dc = local_dc
  return self
end

function _M:init(peers)
  local local_peers, remote_peers = {}, {}

  for i = 1, #peers do
    if peers[i].data_center == self.local_dc then
      local_peers[#local_peers+1] = peers[i]
    else
      remote_peers[#remote_peers+1] = peers[i]
    end
  end

  self.start_local_idx = -2
  self.start_remote_idx = -2
  self.local_peers = local_peers
  self.remote_peers = remote_peers
end

local function next_peer(state, i)
  i = i + 1

  if state.local_tried < #state.local_peers then
    state.local_tried = state.local_tried + 1
    state.local_idx = state.local_idx + 1
    return i, state.local_peers[(state.local_idx % #state.local_peers) + 1]
  elseif state.remote_tried < #state.remote_peers then
    state.remote_tried = state.remote_tried + 1
    state.remote_idx = state.remote_idx + 1
    return i, state.remote_peers[(state.remote_idx % #state.remote_peers) + 1]
  end
end

function _M:iter()
  self.local_tried = 0
  self.remote_tried = 0
  self.local_idx = (self.start_local_idx % #self.local_peers) + 1
  self.remote_idx = (self.start_remote_idx % #self.remote_peers) + 1
  self.start_remote_idx = self.start_remote_idx + 1
  self.start_local_idx = self.start_local_idx + 1
  return next_peer, self, 0
end

return _M
