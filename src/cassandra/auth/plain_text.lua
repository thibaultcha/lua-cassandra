local fmt = string.format

return {
  new = function(self, username, password)
    if username == nil then
      error("No username provided", 3)
    elseif password == nil then
      error("No password provided", 3)
    end

    self.username = username
    self.password = password
  end,
  initial_response = function(self)
    return fmt("\0%s\0%s", self.username, self.password)
  end
}
