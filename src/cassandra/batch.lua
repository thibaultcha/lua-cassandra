--------
-- This module represents a Cassandra batch statement.
-- A batch can combine multiple data modification statements (INSERT, UPDATE, DELETE)
-- into a single operation.
-- A batch is instanciated by the `Cassandra` module.
-- See the related `batch.lua` example.
-- @see http://docs.datastax.com/en/cql/3.1/cql/cql_reference/batch_r.html
-- @module BatchStatement

local batch = {}

batch.__index = batch

batch.is_batch_statement = true

--- Add a query to the batch operation.
-- The query can either be a plain string or a prepared statement.
-- @param query The query or prepared statement to add to the batch.
-- @param args Arguments of the query.
function batch:add(query, args)
  table.insert(self.queries, {query = query, args = args})
end

return batch
