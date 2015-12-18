local function translate(host, port)
  if port then
    return host..":"..port
  else
    return host
  end
end

return translate
