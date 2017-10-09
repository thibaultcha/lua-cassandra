--[[
Deque implementation, taken from the "Programming in Lua" book.
http://www.lua.org/pil/11.4.html
--]]

local _List = {}

function _List.new ()
    return {first = 0, last = -1 }
end

function _List.pushleft (list, value)
    local first = list.first - 1
    list.first = first
    list[first] = value
end

function _List.pushright (list, value)
    local last = list.last + 1
    list.last = last
    list[last] = value
end

function _List.popleft (list)
    local first = list.first
    if first > list.last then return nil, "list is empty" end
    local value = list[first]
    list[first] = nil        -- to allow garbage collection
    list.first = first + 1
    return value, nil
end

function _List.popright (list)
    local last = list.last
    if list.first > last then return nil, "list is empty" end
    local value = list[last]
    list[last] = nil         -- to allow garbage collection
    list.last = last - 1
    return value, nil
end

function _List.length (list)
    return list.last - list.first + 1
end

return _List