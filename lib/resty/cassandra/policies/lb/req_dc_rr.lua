--- Request and Datacenter-aware round robin load balancing policy.
-- This policy will work better than its plain Round Robin counterpart
-- in multi-datacenters setups.
-- It is implemented in such a fashion that it will always prioritize nodes
-- from the local/closest datacenter (which needs to be manually specified).
-- It also gives preference to reusing the same datacenter for the duration of a request
-- @module resty.cassandra_policies.lb.req_dc_rr
-- @author thibaultcha & kikito

local _M = require('resty.cassandra.policies.lb').new_policy('req_and_dc_aware_round_robin')

--- Create a Request-and-DC-aware round robin policy.
-- Instanciates a Request-and-DC-aware round robin policy for `resty.cassandra.cluster`.
--
-- @usage
-- local Cluster = require "resty.cassandra.cluster"
-- local req_dc_rr = require "resty.cassandra.policies.lb.req_dc_rr"
--
-- local policy = req_dc_rr.new("my_local_cluster_name")
-- local cluster = assert(Cluster.new {
--   lb_policy = policy
-- })
--
-- @param[type=string] local_dc Name of the local/closest datacenter.
-- @treturn table `policy`: A DC-aware round robin policy.
function _M.new(local_dc)
  assert(type(local_dc) == 'string', 'local_dc must be a string')

  local self = _M.super.new()
  self.local_dc = local_dc
  return self
end

function _M:init(peers)
  local local_peers, remote_peers = {}, {}

  for i = 1, #peers do
    if type(peers[i].data_center) ~= 'string' then
      error('peer '..peers[i].host..' data_center field must be a string')
    end

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

local function advance_local_or_remote_peer(state)
  if state.local_tried < #state.local_peers then
    state.local_tried = state.local_tried + 1
    state.local_idx = state.local_idx + 1
    return state.local_peers[(state.local_idx % #state.local_peers) + 1]
  end

  if state.remote_tried < #state.remote_peers then
    state.remote_tried = state.remote_tried + 1
    state.remote_idx = state.remote_idx + 1
    return state.remote_peers[(state.remote_idx % #state.remote_peers) + 1]
  end
end

local function next_peer(state, i)
  i = i + 1

  if i == 1 and state.initial_cassandra_coordinator then
    return i, state.initial_cassandra_coordinator
  end

  local peer = advance_local_or_remote_peer(state)
  if not peer then
    return nil
  end

  if state.initial_cassandra_coordinator == peer then
    peer = advance_local_or_remote_peer(state)
    if not peer then
      return nil
    end
  end

  if state.ctx then
    state.ctx.cassandra_coordinator = peer
  end

  return i, peer
end

function _M:iter()
  self.local_tried = 0
  self.remote_tried = 0
  self.ctx = ngx and ngx.ctx
  self.initial_cassandra_coordinator = self.ctx and self.ctx.cassandra_coordinator
  self.local_idx = (self.start_local_idx % #self.local_peers) + 1
  self.remote_idx = (self.start_remote_idx % #self.remote_peers) + 1
  self.start_remote_idx = self.start_remote_idx + 1
  self.start_local_idx = self.start_local_idx + 1
  return next_peer, self, 0
end

return _M
