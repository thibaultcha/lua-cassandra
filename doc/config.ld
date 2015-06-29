project = "lua-cassandra"
title = "lua-cassandra documentation"
description = "A Lua Cassandra client for binary protocol v2 and v3"
readme = "doc/README.md"
format = "markdown"
style = "!pale"
kind_names={topic = "Manual"}
examples = {"doc/examples"}

custom_see_handler("^(%w+):close", function(name)
  if name == "tcpsock" then
    return name..":close()", "http://wiki.nginx.org/HttpLuaModule#tcpsock:close"
  else
    return name..":close()", "http://w3.impa.br/~diego/software/luasocketp.html#close"
  end
end)
