project = "lua-cassandra"
title = "lua-cassandra documentation"
description = "A Lua Cassandra client for binary protocol v2 and v3"
readme = "doc/README.md"
format = "markdown"
style = "!pale"
kind_names={topic = "Manual"}
examples = {"doc/examples"}

custom_see_handler("^(%w+):(%w+)", function(lib, name)
  if lib == "tcpsock" then
    return lib..":"..name.."()", "http://wiki.nginx.org/HttpLuaModule#tcpsock:"..name
  else
    return lib..":"..name.."()", "http://w3.impa.br/~diego/software/luasocket/tcp.html#"..name
  end
end)
