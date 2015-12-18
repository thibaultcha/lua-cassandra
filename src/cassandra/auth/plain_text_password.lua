local string_format = string.format

local PasswordAuthenticator = {}

function PasswordAuthenticator:new(options)
  if options.username == nil then
    return "No username defined in options"
  elseif options.password == nil then
    return "No password defined in options"
  end

  self.username = options.username
  self.password = options.password
end

function PasswordAuthenticator:initial_response()
  return string_format("\0%s\0%s", self.username, self.password)
end

return PasswordAuthenticator
