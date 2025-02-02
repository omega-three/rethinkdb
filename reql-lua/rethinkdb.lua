local cjson = require("cjson")
local base64 = require("base64")
local connection = require("reql-lua.connection")
local protocol = require("reql-lua.protocol")
local reql = require("reql-lua.reql")

local M = {
    params = {
        host = "127.0.0.1",
        port = 28015,
        user = "admin",
        password = "",
        db = "test",
        timeout = 20,
        max_retries = 3
    },
    json = cjson,
    base64 = base64
}
M.__index = M

function M:init(properties)
    local self = setmetatable({}, M)

    if type(properties) == "table" then
        for property, value in pairs(properties) do
            self.params[property] = value
        end
    end

    connection:init(self.params):assign(self)
    protocol:init(self)
    reql:init(self)

    return self
end

return M