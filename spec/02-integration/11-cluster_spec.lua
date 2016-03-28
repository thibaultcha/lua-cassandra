local utils = require "spec.spec_utils"
local Cluster = require "cassandra.cluster"

-- TODO: only to get cql_errors.
-- This will later be require "cassandra"
local host = require "cassandra.host"

-- TODO: attach type serializers to host
local cassandra = require "cassandra"

describe("cluster", function()
  setup(function()
    utils.ccm_start(3)
  end)

  describe("new()", function()
    it("creates a cluster with default options", function()
      local cluster = assert(Cluster.new())
      assert.same({"127.0.0.1"}, cluster.contact_points)
      assert.is_nil(cluster.keyspace)
    end)
    it("accepts options", function()
      local cluster = assert(Cluster.new {
        contact_points = {"127.0.0.2", "127.0.0.3"},
        keyspace = "system"
      })
      assert.same({"127.0.0.2", "127.0.0.3"}, cluster.contact_points)
      assert.equal("system", cluster.keyspace)
    end)
  end)

  describe("get_first_coordinator()", function()
    it("retrieves the first coordinator to respond", function()
      local cluster = assert(Cluster.new {
        connect_timeout = 100
      })

      local peer = assert(cluster:get_first_coordinator {"127.0.0.255", "127.0.0.1"})
      local rows = assert(peer:execute "SELECT * FROM system.peers")
      assert.equal(2, #rows)

      finally(function()
        peer:close()
      end)
    end)
    it("returns nil when no coordinator replied", function()
      local cluster = assert(Cluster.new {
        connect_timeout = 100
      })

      local peer, err = cluster:get_first_coordinator {"127.0.0.254", "127.0.0.255"}
      assert.equal("all hosts tried for query failed. 127.0.0.254: timeout 127.0.0.255: timeout", err)
      assert.is_nil(peer)
    end)
  end)

  describe("refresh()", function()
    it("refreshes cluster infos in shm", function()
      local cluster = assert(Cluster.new())

      assert(cluster:refresh())

      local cluster_infos = assert(cluster:peers())
      assert.same({"127.0.0.3", "127.0.0.2", "127.0.0.1"}, cluster_infos)

      for _, host in ipairs(cluster_infos) do
        local peer_infos = assert(cluster:get_peer(host))
        assert.same({reconnection_delay = 0, unhealthy_at = 0}, peer_infos)
      end
    end)
    it("complains when no coordinator replied", function()
      local cluster = assert(Cluster.new {
        contact_points = {"127.0.0.254", "127.0.0.255"},
        connect_timeout = 100
      })

      local ok, err = cluster:refresh()
      assert.is_nil(ok)
      assert.equal("all hosts tried for query failed. 127.0.0.254: timeout 127.0.0.255: timeout", err)
    end)
  end)

  describe("get_next_coordinator()", function()
    it("complains if no hosts are in shm", function()
      local cluster = assert(Cluster.new())

      local peer, err = cluster:get_next_coordinator()
      assert.is_nil(peer)
      assert.equal("no hosts to try, must refresh", err)
    end)
    it("retrieves the next healthy peer from the load balancing policy", function()
      -- default is shm round robin policy
      local cluster = assert(Cluster.new())
      assert(cluster:refresh())

      local peer_1 = assert(cluster:get_next_coordinator())
      local rows = assert(peer_1:execute "SELECT * FROM system.peers")
      assert.equal(2, #rows)

      local peer_2 = assert(cluster:get_next_coordinator())
      rows = peer_2:execute "SELECT * FROM system.peers"
      assert.equal(2, #rows)

      assert.not_equal(peer_1.host, peer_2.host)

      finally(function()
        peer_1:close()
        peer_2:close()
      end)
    end)
  end)

  describe("execute()", function()
    it("refreshes automatically if needed", function()
      local cluster = assert(Cluster.new())

      local rows = assert(cluster:execute "SELECT * FROM system.peers")
      assert.equal(2, #rows)
    end)
    it("spawns hosts in a keyspace", function()
      local cluster = assert(Cluster.new {keyspace = "system"})
      local rows = assert(cluster:execute "SELECT * FROM peers")
      assert.equal(2, #rows)
    end)
    it("prepares and execute at once", function()
      local query = "SELECT * FROM system.peers"
      local cluster = assert(Cluster.new {query_options = {prepared = true}})
      local rows = assert(cluster:execute(query))
      assert.equal(2, #rows)

      assert(cluster:get_prepared(query))
    end)
    it("returns CQL errors", function()
      local cluster = assert(Cluster.new())
      local res, err, code = cluster:execute "SELECT"
      assert.is_nil(res)
      assert.equal("[Syntax error] line 0:-1 no viable alternative at input '<EOF>'", err)
      assert.truthy(code)
      assert.equal(host.cql_errors.SYNTAX_ERROR, code)
    end)
    it("returns request infos", function()
      local cluster = assert(Cluster.new())

      local rows, _, request_infos = assert(cluster:execute "SELECT * FROM system.peers")
      assert.equal(2, #rows)
      assert.equal(0, request_infos.n_retries)
      assert.is_string(request_infos.coordinator)
    end)

    describe("load balancing policy", function()
      it("defaults to shared_round_robin", function()
        local lb_policies = require "cassandra.policies.load_balancing"
        local cluster = assert(Cluster.new())
        assert.equal(lb_policies.shared_round_robin, cluster.load_balancing_policy)
      end)
      it("selects the coordinator from the load balancing policy", function()
        -- default is shm round robin policy
        local cluster = assert(Cluster.new())

        local s = spy.on(cluster, "get_next_coordinator")

        for i = 1, 3 do
          local rows = assert(cluster:execute "SELECT * FROM system.peers")
          assert.equal(2, #rows)
          assert.spy(s).was.called(i)
        end
      end)
      it("sets unavailable nodes as down", function()
        local cluster = assert(Cluster.new())
        assert(cluster:refresh())
        assert(3, #cluster.hosts)
        finally(function()
          utils.ccm_up_node(1)
        end)

        -- Stop a node
        utils.ccm_down_node(1)

        -- Now 127.0.0.1 should be skipped
        local tracked_nodes, uniques = {}, 0
        for i = 1, 3 do
          local rows, _, request_infos = assert(cluster:execute "SELECT * FROM system.peers")
          assert.equal(2, #rows)
          tracked_nodes[request_infos.coordinator] = true
        end
        for k in pairs(tracked_nodes) do uniques = uniques + 1 end
        assert.equal(2, uniques)
        assert.is_nil(tracked_nodes["127.0.0.1"])

        local peer_infos = assert(cluster:get_peer("127.0.0.1"))
        assert.True(peer_infos.unhealthy_at > 0)
      end)
    end)

    describe("reconnection policy", function()
      it("defaults to shared_exp", function()
        local cluster = assert(Cluster.new())
        assert.equal("shared_exp", cluster.reconnection_policy.name)
      end)
      it("reconnects exponentially #slow", function()
        local cluster = assert(Cluster.new())
        assert(cluster:refresh())
        assert(3, #cluster.hosts)
        finally(function()
          utils.ccm_up_node(1)
        end)

        -- Stop a node
        utils.ccm_down_node(1)

        -- Now 127.0.0.1 should be skipped
        for i = 1, 3 do
          local rows = assert(cluster:execute "SELECT * FROM system.peers")
          assert.equal(2, #rows)
        end

        -- reconnection delay should be at its 1st iteration
        local peer_infos = assert(cluster:get_peer("127.0.0.1"))
        local delay_1 = cluster.reconnection_policy.get_next("stub")
        assert.equal(delay_1, peer_infos.reconnection_delay)

        os.execute("sleep "..delay_1/1000)

        for i = 1, 3 do
          local rows = assert(cluster:execute "SELECT * FROM system.peers")
          assert.equal(2, #rows)
        end
        -- 2nd iteration
        peer_infos = assert(cluster:get_peer("127.0.0.1"))
        local delay_2 = cluster.reconnection_policy.get_next("stub")
        assert.equal(delay_2, peer_infos.reconnection_delay)

        assert.not_equal(delay_1, delay_2)

        -- node back up
        utils.ccm_up_node(1)

        os.execute("sleep "..delay_2/1000)

        for i = 1, 3 do
          local rows = assert(cluster:execute "SELECT * FROM system.peers")
          assert.equal(2, #rows)
        end

        peer_infos = assert(cluster:get_peer("127.0.0.1"))
        assert.equal(0, peer_infos.reconnection_delay)
        assert.equal(0, peer_infos.unhealthy_at)
      end)
    end)

    describe("retry policy", function()
      it("defaults to simple retry", function()
        local cluster = assert(Cluster.new())
        assert.equal(3, cluster.retry_policy.max_retries)
      end)
    end)

    describe("CQL error: unprepared", function()
      local uuid, peer = "ca002f0a-8fe4-11e5-9663-43d80ec97d3e"
      math.randomseed(os.time())
      local r = math.random(10^8)
      setup(function()
        local p = assert(host.new())
        assert(p:connect())
        assert(utils.create_keyspace(p, utils.keyspace))
        assert(p:set_keyspace(utils.keyspace))
        assert(p:execute [[
          CREATE TABLE IF NOT EXISTS foos(
            id uuid,
            n int,
            PRIMARY KEY(id, n)
          )
        ]])
        assert(p:execute("INSERT INTO foos(id, n) VALUES(?, ?)", {cassandra.uuid(uuid), r}))

        peer = p
      end)
      teardown(function()
        peer:close()
      end)
      it("prepare and retry on a node that did not have the prepared query", function()
        -- prepare a dumb query (dumb, but unique, so no need to restart the node for this test)
        local query = "SELECT * FROM foos WHERE id = ? AND n = "..r
        local cluster = assert(Cluster.new {
          keyspace = utils.keyspace,
          query_options = {prepared = true}
        })

        -- on 1st call, query will be prepared, but the second coordinator will not have
        -- this query prepared
        local ok = false
        for i = 1, 3 do
          local rows, _, request_infos = assert(cluster:execute(query, {cassandra.uuid(uuid)}))
          assert.equal(1, #rows)
          if request_infos.prepared_and_retried then
            assert.equal(0, request_infos.n_retries)
            assert.equal(query, request_infos.orig_query)
            ok = true
          end
        end

        assert.True(ok)
      end)
    end)

    describe("connection error: timeout", function()
      local get_next_coordinator_orig = Cluster.get_next_coordinator
      before_each(function()
        Cluster.get_next_coordinator = function(self)
          -- revert this stub, allowing retry policy to step in and chose another node
          self.get_next_coordinator = get_next_coordinator_orig
          return self.stub_coordinator
        end
      end)
      after_each(function()
        Cluster.get_next_coordinator = get_next_coordinator_orig
      end)
      it("accepts a false retry_on_timeout option", function()
        local cluster = assert(Cluster.new {retry_on_timeout = false})
        assert.False(cluster.retry_on_timeout)
      end)
      it("retries if host times out", function()
        finally(function()
          utils.ccm_up_node(1)
        end)

        local cluster = assert(Cluster.new {
          connect_timeout = 100,
          read_timeout = 100
        })

        assert(cluster:refresh()) -- retrieve all hosts to test the retry policy later

        cluster.stub_coordinator = assert(host.new {host = "127.0.0.1"}) -- create a valid peer and open its connection
        cluster.stub_coordinator:settimeout(100)
        assert(cluster.stub_coordinator:connect()) -- force this coordinator to be used first by the stub cluster
        spy.on(cluster.stub_coordinator, "setkeepalive")

        utils.ccm_down_node(1) -- simulate node going down

        local test_peer = assert(host.new {host = "127.0.0.1"}) -- make sure this host really times out first
        test_peer:settimeout(100)
        local _, err = test_peer:connect()
        assert.equal("timeout", err)

        -- Attempt request (forcing our poisoned coordinator over the load balancing policy)
        for i = 1, 3 do
          local rows = assert(cluster:execute "SELECT * FROM system.local")
          assert.equal(1, #rows)
          for _, row in ipairs(rows) do
            assert.not_equal("127.0.0.1", row.rpc_address) -- we never hit this node, it's the one which timed out
          end
        end

        assert.spy(cluster.stub_coordinator.setkeepalive).was_called(1)
      end)
      it("does not retry without retry_on_timeout", function()
        finally(function()
          utils.ccm_up_node(1)
        end)

        local cluster = assert(Cluster.new {
          connect_timeout = 100,
          read_timeout = 100,
          retry_on_timeout = false
        })

        assert(cluster:refresh()) -- retrieve all hosts to test the retry policy later

        cluster.stub_coordinator = assert(host.new {host = "127.0.0.1"}) -- create a valid peer and open its connection
        cluster.stub_coordinator:settimeout(100)
        assert(cluster.stub_coordinator:connect()) -- force this coordinator to be used first by the stub cluster
        spy.on(cluster.stub_coordinator, "setkeepalive")

        utils.ccm_down_node(1) -- simulate node going down

        local test_peer = assert(host.new {host = "127.0.0.1"}) -- make sure this host really times out first
        test_peer:settimeout(100)
        local _, err = test_peer:connect()
        assert.equal("timeout", err)

        -- Attempt request (forcing our poisoned coordinator over the load balancing policy)
        assert.error_matches(function()
          for i = 1, 3 do
            assert(cluster:execute "SELECT * FROM system.local")
          end
        end, "timeout")

        assert.spy(cluster.stub_coordinator.setkeepalive).was_called(1)
      end)
    end)

    describe("schema consensus", function()
      it("waits on SCHEMA_CHANGE results", function()
        local cluster = assert(Cluster.new {keyspace = utils.keyspace})
        finally(function()
          cluster:execute "DROP TABLE consensus"
        end)

        local res = assert(cluster:execute [[
          CREATE TABLE consensus(id int PRIMARY KEY)
        ]])
        assert.equal("SCHEMA_CHANGE", res.type)
      end)
      it("timeouts", function()
        local cluster = assert(Cluster.new {
          keyspace = utils.keyspace,
          max_schema_consensus_wait = 1000
        })
        finally(function()
          cluster:execute "DROP TABLE consensus"
        end)

        local res, err = cluster:execute [[
          CREATE TABLE consensus(id int PRIMARY KEY)
        ]]
        assert.equal("error while waiting for schema consensus: timeout", err)
        assert.is_nil(res)
      end)
    end)
  end) -- execute()

  describe("batch()", function()
    local peer, cluster
    setup(function()
      cluster = assert(Cluster.new {keyspace = utils.keyspace})
      peer = assert(host.new())
      assert(peer:connect())
      assert(peer:set_keyspace(utils.keyspace))
      assert(peer:execute [[
        CREATE TABLE IF NOT EXISTS things2(
          id int PRIMARY KEY,
          n int
        )
      ]])
    end)
    teardown(function()
      assert(peer:execute "TRUNCATE things2")
      peer:close()
      cluster:shutdown()
    end)
    it("executes a batch with auto-refresh and LB policy", function()
      local res, _, request_infos1 = assert(cluster:batch {
        "INSERT INTO things2(id, n) VALUES(1, 1)",
        "UPDATE things2 SET n = 2 WHERE id = 1"
      })
      assert.equal("VOID", res.type)
      assert.is_string(request_infos1.coordinator)

      local res, _, request_infos2 = assert(cluster:batch {
        {"UPDATE things2 SET n = 3 WHERE id = 1"}
      })
      assert.equal("VOID", res.type)
      assert.is_string(request_infos2.coordinator)

      assert.not_equal(request_infos2.coordinator, request_infos1.coordinator)
      local rows = assert(cluster:execute "SELECT * FROM things2 WHERE id = 1")
      assert.equal(3, rows[1].n)
    end)
    it("executes a prepared batch without args", function()
      local res, _, request_infos = assert(cluster:batch({
        "INSERT INTO things2(id, n) VALUES(2, 1)",
        "UPDATE things2 SET n = 2 WHERE id = 2"
      }, {prepared = true}))
      assert.equal("VOID", res.type)
      assert.True(request_infos.prepared)

      local rows = assert(cluster:execute "SELECT * FROM things2 WHERE id = 2")
      assert.equal(2, rows[1].n)
    end)
    it("executes a prepared batch with args", function()
      local res, _, request_infos = assert(cluster:batch({
        {"INSERT INTO things2(id, n) VALUES(3, ?)", {1}},
        {"UPDATE things2 SET n = ? WHERE id = 3", {2}}
      }, {prepared = true}))
      assert.equal("VOID", res.type)
      assert.True(request_infos.prepared)

      local rows = assert(cluster:execute "SELECT * FROM things2 WHERE id = 3")
      assert.equal(2, rows[1].n)
    end)
  end)

  describe("iterate()", function()
    local n_inserts, n_select, peer = 1001, 20
    setup(function()
      peer = assert(host.new())
      assert(peer:connect())
      assert(peer:set_keyspace(utils.keyspace))
      assert(peer:execute [[
        CREATE TABLE IF NOT EXISTS metrics(
          id int PRIMARY KEY,
          n int
        )
      ]])
      assert(peer:execute "TRUNCATE metrics")
      for i = 1, n_inserts do
        assert(peer:execute("INSERT INTO metrics(id,n) VALUES(?,?)", {i, i*i}))
      end
    end)
    it("iterates with auto-refresh", function()
      local cluster = assert(Cluster.new {keyspace = utils.keyspace})
      local s = spy.on(cluster, "get_next_coordinator")
      local n_page = 0
      local opts, buf = {page_size = n_select}, {}
      for rows, err, page in cluster:iterate("SELECT * FROM metrics", nil, opts) do
        assert.is_nil(err)
        assert.is_number(page)
        assert.is_table(rows)
        assert.equal("ROWS", rows.type)
        n_page = n_page + 1
        for _, v in ipairs(rows) do buf[#buf+1] = v end
      end

      assert.equal(n_inserts, #buf)
      assert.equal(math.ceil(n_inserts/n_select), n_page)
      assert.spy(s).was_called(n_page)
    end)
  end)

  describe("shutdown()", function()
    it("flushes all the data in shms", function()
      local cluster = assert(Cluster.new())
      assert(cluster:refresh())

      local keys = cluster.shm:get_keys()
      assert.not_same({}, keys)

      cluster:shutdown()

      keys = cluster.shm:get_keys()
      assert.same({}, keys)
    end)
  end)
end)
