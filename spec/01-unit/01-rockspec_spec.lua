local _setfenv = setfenv

local function exec(cmd)
  local tmp = os.tmpname()
  os.execute(cmd.." > "..tmp)
  local f = assert(io.open(tmp, "r"))
  local stdout = f:read("*a")
  os.remove(tmp)
  return stdout
end

if not _setfenv then
  _setfenv = function(fn, env)
    local i = 1
    while true do
      local name = debug.getupvalue(fn, i)
      if name == "_ENV" then
        debug.upvaluejoin(fn, i, function()
          return env
        end, 1)
        break
      elseif not name then
        break
      end
      i = i + 1
    end
    return fn
  end
end

describe("rockspec", function()
  local rock, lua_srcs = {}, {}
  local rock_filename

  setup(function()
    local files = exec("find ./lib -name *.lua")
    for str in string.gmatch(files, "(.-)\n") do
      lua_srcs[#lua_srcs+1] = str
    end

    rock_filename = exec("find . -name lua-cassandra-*.rockspec")
    local f = assert(loadfile(rock_filename:sub(1, -2)))
    _setfenv(f, rock)
    f()
  end)

  describe("modules", function()
    it("are all included", function()
      for _, src in ipairs(lua_srcs) do
        src = src:sub(3) -- strip './'
        local found
        for mod_name, mod_path in pairs(rock.build.modules) do
          if mod_path == src then
            found = true
            break
          end
        end
        assert(found, "could not find module entry for Lua file: "..src)
      end
    end)
    it("all modules named as their path", function()
      for mod_name, mod_path in pairs(rock.build.modules) do
          mod_path = mod_path:gsub("%.lua$", ""):gsub("lib/", ""):gsub("/", '.'):gsub("%.init$", "")
          assert(mod_name == mod_path, mod_path.." has a different name ("..mod_name..")")
      end
    end)
  end)
end)
