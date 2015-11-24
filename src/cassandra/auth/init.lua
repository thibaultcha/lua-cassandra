local AUTHENTICATORS = {
  ["org.apache.cassandra.auth.PasswordAuthenticator"] = "cassandra.auth.plain_text_password"
}

local function new_authenticator(class_name, options)
  local auth_module = AUTHENTICATORS[class_name]
  if auth_module == nil then
    return nil, "No authenticator implemented for class "..class_name
  end

  local authenticator = require(auth_module)
  authenticator.__index = authenticator
  setmetatable({}, authenticator)
  local err = authenticator:new(options)
  return authenticator, err
end

return {
  new_authenticator = new_authenticator
}
