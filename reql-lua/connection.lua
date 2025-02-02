local json = require("cjson")
local base64 = require("base64")
local p_sock = require("posix.sys.socket")
local p_unistd = require("posix.unistd")
local pt = require("posix.time")
local openssl_rand = require("openssl.rand")
local openssl_kdf = require("openssl.kdf")
local openssl_hmac = require("openssl.hmac")
local openssl_digest = require("openssl.digest")

local have_bitwise, bit = pcall(require, "reql-lua.bitwise") ---Lua 5.3+
if not have_bitwise then
    if _G.bit32 then ---Lua 5.2
        bit = _G.bit32
    elseif _G.bit and _G.jit then ---LuaJIT
        bit = _G.bit
    else ---Lua 5.1
        bit = require("bit32")
    end
end

local unpack = _G.unpack or table.unpack

local function int_to_le(num, size)
    local bytes = {}
    for i = 1, size do
        bytes[i] = string.char(num % 256)
        num = math.floor(num / 256)
    end
    return table.concat(bytes)
end

local function le_to_int(str)
    local num = 0
    for i = str:len(), 1, -1 do
        num = num * 256 + string.byte(str, i)
    end
    return num
end

local function bxor256(u, t)
    local res = {}
    for i = 1, math.max(string.len(u), string.len(t)) do
        res[i] = bit.bxor(string.byte(u, i) or 0, string.byte(t, i) or 0)
    end
    return string.char(unpack(res))
end

local function compare_digest(a, b)
    local res
    if string.len(a) == string.len(b) then
        res = 0
    end
    if string.len(a) ~= string.len(b) then
        res = 1
    end
    for i=1, math.max(string.len(a), string.len(b)) do
        res = bit.bor(res, bit.bxor(string.byte(a, i) or 0, string.byte(b, i) or 0))
    end
    return res ~= 0
end

local M = {
    params = {}
}
M.__index = M

function M:init(params)
    local self = setmetatable({}, M)

    if type(params) == "table" then
        for param, value in pairs(params) do
            self.params[param] = value
        end
    end

    self.sock_d = nil
    self.connected = false

    return self
end

function M:assign(r)
    r.connection = self
    return self
end

function M:connect()
    local address, err, ok, sa

    address, err = p_sock.getaddrinfo(self.params.host, "http", {family = p_sock.AF_INET, socktype = p_sock.SOCK_STREAM})
    if not address then
        return nil, err
    end

    address = {
        addr = address[1].addr,
        port = self.params.port,
        family = address[1].family,
        socktype = address[1].socktype,
        protocol = p_sock.IPPROTO_TCP
    }

    self.sock_d, err = p_sock.socket(p_sock.AF_INET, p_sock.SOCK_STREAM, 0)
    if err then
        return nil, err
    end

    ok, err = p_sock.setsockopt(self.sock_d, p_sock.SOL_SOCKET, p_sock.SO_SNDTIMEO, self.params.timeout, 0)
    ok, err = p_sock.setsockopt(self.sock_d, p_sock.SOL_SOCKET, p_sock.SO_RCVTIMEO, self.params.timeout, 0)
    ok, err = p_sock.setsockopt(self.sock_d, p_sock.SOL_SOCKET, p_sock.SO_KEEPALIVE, 1)

    ok, err = p_sock.connect(self.sock_d, address) --0, if successful
    if err then
        return nil, err
    end

    sa, err = p_sock.getsockname(self.sock_d)
    if err then
        return nil, err
    end

    return self:handshake()
end

function M:handshake()
    local ok, response

    ok, response = self:send_message(string.char(0xc3, 0xbd, 0xc2, 0x34), false)
    if not ok then
        return nil, "Failed magick number response: " .. response
    end
    if not response.success then
        return nil, "Failed magick number response: " .. json.encode(response)
    end

    local client_nonce = base64.encode(openssl_rand.bytes(18))
    local client_first_auth = "n=".. self.params.user ..",r=" .. client_nonce
    local client_first_message = {
        protocol_version = response.min_protocol_version,
        authentication_method = "SCRAM-SHA-256",
        authentication = "n,," .. client_first_auth
    }

    ok, response = self:send_message(json.encode(client_first_message), true)
    if not ok then
        return nil, "Failed server first message: " .. response
    end
    if not response.success then
        return nil, "Failed server first message: " .. response.error_code .. "/" .. response.error
    end

    local server_first_message = response.authentication
    local authentication = {}
    for k, v in (server_first_message .. ","):gmatch('([rsi])=(.-),') do
        authentication[k] = v
    end

    if authentication.r:sub(1, client_nonce:len()) ~= client_nonce then
        return nil, "Server reply invalid nonce"
    end

    local salt = base64.decode(authentication.s)
    local salted_password = openssl_kdf.derive({
        type = "PBKDF2",
        md = "sha256",
        salt = salt,
        iter = tonumber(authentication.i),
        pass = self.params.password,
        outlen = 32
    })

    if not salted_password then
        return nil, "Salted password error"
    end

    local client_key = openssl_hmac.new(salted_password, "sha256"):update('Client Key'):final()
    local stored_key = openssl_digest.new("sha256"):final(client_key)

    local client_final_message_no_proof = "c=biws,r=".. authentication.r
    local auth_message = table.concat({
        client_first_auth,
        server_first_message,
        client_final_message_no_proof
    }, ',')

    local client_signature = openssl_hmac.new(stored_key, "sha256"):update(auth_message):final()
    local client_proof = bxor256(client_key, client_signature)
    local server_key = openssl_hmac.new(salted_password, "sha256"):update('Server Key'):final()
    local server_signature = openssl_hmac.new(server_key, "sha256"):update(auth_message):final()

    local client_final_message = {
        authentication = client_final_message_no_proof .. ",p=" .. base64.encode(client_proof)
    }

    ok, response = self:send_message(json.encode(client_final_message), true)
    if not ok then
        return nil, "Failed server final message: " .. response
    end
    if not response.success then
        return nil, "Failed server final message: " .. response.error_code .. "/" .. response.error
    end

    local server_final_message = response.authentication
    for k, v in server_final_message:gmatch('([v])=(.+)') do
        authentication[k] = v
    end

    if not authentication.v then
        return nil, "Missing server signature"
    end

    if compare_digest(authentication.v, server_signature) then
        self.connected = true
        return true
    end

    return nil, "Invalid server signature"
end

function M:send_message(msg, null_terminated)
    local ok, err = p_sock.send(self.sock_d, msg .. (null_terminated and "\0" or ""))
    if not ok then
        return nil, err
    end

    local data = {}
    local timeout
    while true do
        local buf = p_sock.recv(self.sock_d, 1)
        if not buf or buf == "\0" then
            break
        end

        table.insert(data, buf)

        if buf:len() == 0 then
            if not timeout then
                timeout = pt.time() + self.params.timeout
            end
            if timeout < pt.time() then
                return nil, "Response waiting timeout reached"
            end
        end
    end
    data = table.concat(data)
    return pcall(json.decode, data)
end

function M:send_query(token, data)
    local query_token = int_to_le(token, 8)
    local packed_size = int_to_le(data:len(), 4)
    local final_query = table.concat({query_token, packed_size, data})

    local ok, err = p_sock.send(self.sock_d, final_query)
    if not ok then
        self:close()
        return {
            error = err
        }
    end

    local _token, _length, timeout
    local response, raw = "", ""

    while true do
        local buf = p_sock.recv(self.sock_d, 4096)
        if not buf then
            break
        end

        raw = raw .. buf

        if raw:len() >= 12 then
            _token = le_to_int(raw:sub(1, 8))
            _length = le_to_int(raw:sub(9, 12))
            if raw:len() > 12 then
                response = raw:sub(13)
                if _length == response:len() then
                    break
                end
            end
        end

        if buf:len() < 12 then
            if not timeout then
                timeout = pt.time() + self.params.timeout
            end
            if timeout < pt.time() then
                self:close()
                return {
                    error = "Response waiting timeout reached"
                }
            end
        end
    end

    return {
        token = _token,
        response = response
    }
end

function M:close()
    p_unistd.close(self.sock_d)
    self.connected = false
    self.sock_d = nil
end

return M