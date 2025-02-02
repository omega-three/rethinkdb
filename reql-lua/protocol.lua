local protodef = require("reql-lua.proto-def")

local Query = protodef.Query
local Term = protodef.Term
local datum = Term.datum
local make_obj = Term.make_obj

local CONTINUE = '[' .. Query.CONTINUE .. ']'
local NOREPLY_WAIT = '[' .. Query.NOREPLY_WAIT .. ']'
local SERVER_INFO = '[' .. Query.SERVER_INFO .. ']'
local STOP = '[' .. Query.STOP .. ']'

local START = Query.START

local local_opts = {
    binary_format = true,
    format = true,
    group_format = true,
    time_format = true,
}

--- convert from internal representation to JSON
local function build(term)
    if type(term) ~= 'table' then
        return term
    end
    if term.tt == datum then
        return term.args[1]
    end
    if term.tt == make_obj then
        local res = {}
        for key, val in pairs(term.optargs) do
            res[key] = build(val)
        end
        return res
    end
    local res = {term.tt}
    if next(term.args) then
        local args = {}
        for i, arg in ipairs(term.args) do
            args[i] = build(arg)
        end
        res[2] = args
    end
    if next(term.optargs) then
        local opts = {}
        for key, val in pairs(term.optargs) do
            opts[key] = build(val)
        end
        res[3] = opts
    end
    return res
end

local function write_socket(r, conn, token, data)
    local res = conn:send_query(token, data)
    if res.token and res.response and res.response:len()>0 then
        local success, params = pcall(r.json.decode, res.response)
        if success then
            return {
                token = res.token,
                response = params
            }
        end
    end
    return nil, "Socket write failed: " .. (res.error and res.error or "incorrect response")
end

local M = {}

function M:init(r)
    self.token = 0
    r.protocol = self
end

function M:get_token()
    self.token = self.token + 1
    return self.token
end

function M:send_query(r, conn, reql_inst, options)
    local global_opts = {}

    for first, second in pairs(options) do
        local data, err = r.reql(second)
        if not data then
            return nil, err
        end
        global_opts[first] = data
    end

    if options.db then
        global_opts.db = r.reql.db(global_opts.db)
    else
        global_opts.db = r.reql.db(conn.params.db)
    end

    local query = {START, build(reql_inst)}
    if global_opts and next(global_opts) then
        local optargs = {}
        for k, v in pairs(global_opts) do
            if not local_opts[k] then
                optargs[k] = build(v)
            end
        end
        if next(optargs) then query[3] = optargs end
    end

    local ok, data = pcall(r.json.encode, query)
    if not ok then
        return nil, data
    end

    return write_socket(r, conn, self:get_token(), data)
end

function M:continue_query(r, conn, token)
    return write_socket(r, conn, token, CONTINUE)
end

function M:end_query(r, conn, token)
    return write_socket(r, conn, token, STOP)
end

function M:noreply_wait(r, conn)
    return write_socket(r, conn, self:get_token(), NOREPLY_WAIT)
end

function M:server_info(r, conn)
    return write_socket(r, conn, self:get_token(), SERVER_INFO)
end

return M