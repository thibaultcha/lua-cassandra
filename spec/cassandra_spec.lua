local cassandra = require "cassandra"

describe("Cassandra", function()
  it("should have type annotation shortands", function()
    assert.has_no_error(function()
      cassandra.uuid()
    end)
  end)
end)
