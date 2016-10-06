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
  local rocks = {
    production = {},
    dev = {}
  }
  local lua_srcs = {}
  local rock_filenames

  setup(function()
    local files = exec("find ./lib -name *.lua")
    for str in string.gmatch(files, "(.-)\n") do
      lua_srcs[#lua_srcs+1] = str
    end

    rock_filenames = exec("find . -name 'lua-cassandra-*.rockspec'")
    local rock_filename, dev_rock_filename = rock_filenames:match("(%S.*%S)\n(%S.*%S)")

    local f = assert(loadfile(rock_filename))
    _setfenv(f, rocks.production)
    f()

    f = assert(loadfile(dev_rock_filename))
    _setfenv(f, rocks.dev)
    f()
  end)

  it("meta in sync between both rockspec", function()
    assert.equal(rocks.production.package, rocks.dev.package)
  end)

  describe("description section", function()
    it("is in sync between both rockspec", function()
      assert.same(rocks.production.description, rocks.dev.description)
    end)
  end)

  describe("dependencies", function()
    it("is in sync between both rockspec", function()
      assert.same(rocks.production.dependencies, rocks.dev.dependencies)
    end)
  end)

  for rock_name, rock in pairs(rocks) do
    describe(rock_name .. " rockspec", function()
      describe("source section", function()
        if rock_name == "production" then
          it("has a tag", function()
            assert.is_string(rocks.production.source.tag)
          end)
        else
          it("has no tag", function()
            assert.is_nil(rocks.dev.source.tag)
          end)
        end
      end)

      describe("modules section", function()
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
  end
end)
