--------
--- Example of batch statements execution.

-- Create a batch statement
local batch = cassandra:BatchStatement()

-- Add a query
batch:add("INSERT INTO users (name, age, user_id) VALUES (?, ?, ?)",
          {"James", 32, cassandra.uuid("2644bada-852c-11e3-89fb-e0b9a54a6d93")})

-- Add a prepared statement
local stmt, err = session:prepare("INSERT INTO users (name, age, user_id) VALUES (?, ?, ?)")
batch:add(stmt, {"John", 45, cassandra.uuid("1144bada-852c-11e3-89fb-e0b9a54a6d11")})

-- Execute the batch
local result, err = session:execute(batch)
