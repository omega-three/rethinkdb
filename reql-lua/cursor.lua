local protodef = require("reql-lua.proto-def")

local Response = protodef.Response

local basic_responses = {
    [Response.SERVER_INFO] = true,
    [Response.SUCCESS_ATOM] = true,
    [Response.SUCCESS_SEQUENCE] = true,
    [Response.SUCCESS_PARTIAL] = true
}

local error_types = {
    [Response.COMPILE_ERROR] = 'ReQLCompileError',
    [Response.CLIENT_ERROR] = 'ReQLClientError',
    [Response.RUNTIME_ERROR] = 'ReQLRuntimeError',
}

local function convert_pseudo_type(r, row, options)
    local function native_group(obj)
        assert(obj.data, 'pseudo-type GROUPED_DATA table missing expected field `data`')
        for i = 1, #obj.data do
            obj.data[i] = {group = obj.data[i][1], reduction = obj.data[i][2]}
        end
        return obj.data
    end

    local function native_time(obj)
        local epoch_time = assert(obj.epoch_time, 'pseudo-type TIME table missing expected field `epoch_time`')
        local time = os.date("!*t", math.floor(epoch_time))
        time.timezone = obj.timezone
        return time
    end

    local function native_binary(obj)
        return r.base64.decode('' .. assert(obj.data, 'pseudo-type BINARY table missing expected field `data`'))
    end

    local function raw(obj)
        return obj
    end

    local group_table = {
        native = native_group,
        raw = raw
    }

    local time_table = {
        native = native_time,
        raw = raw
    }

    local binary_table = {
        native = native_binary,
        raw = raw
    }

    local format = options.format or 'raw'
    local binary_format, group_format, time_format =
    options.binary_format or format,
    options.group_format or format,
    options.time_format or format

    local BINARY, GROUPED_DATA, TIME =
    binary_table[binary_format],
    group_table[group_format],
    time_table[time_format]

    if not BINARY then
        return nil, 'Unknown binary_format run option ' .. binary_format
    end

    if not GROUPED_DATA then
        return nil, 'Unknown group_format run option ' .. group_format
    end

    if not TIME then
        return nil, 'Unknown time_format run option ' .. time_format
    end

    local conversion = {
        BINARY = BINARY,
        GEOMETRY = raw,
        GROUPED_DATA = GROUPED_DATA,
        TIME = TIME,
    }

    local function convert(obj)
        if type(obj) == 'table' then
            for key, value in pairs(obj) do
                obj[key] = convert(value)
            end

            -- An R_OBJECT may be a regular table or a 'pseudo-type' so we need a
            -- second layer of type switching here on the obfuscated field '$reql_type$'
            local converter = conversion[obj['$reql_type$']]

            if converter then
                return converter(obj)
            end
        end
        return obj
    end

    local success, data = pcall(convert, row)
    if success then
        return data
    end
    return nil, data
end

local M = {}
M.__index = M

function M:init()
    local self = setmetatable({}, M)
    self.status = "ready"
    self.results = {}
    return self
end

function M:open(r, conn, reql_inst, options)
    self.status = "open"
    self.results = {}
    local data, err = r.protocol:send_query(r, conn, reql_inst, options)
    if not data then
        self:close()
        return nil, err
    end
    return self:process(r, conn, data, options)
end

function M:process(r, conn, data, options)
    local success, need_continue, chunk = self:parse(r, data.response, options)
    if not success then
        self:close()
        return nil, chunk
    end
    for _, item in pairs(chunk) do
        table.insert(self.results, item)
    end
    if not need_continue then
        r.protocol:end_query(r, conn, data.token)
        local results = {}
        for k, v in pairs(self.results) do
            results[k] = v
        end
        self:close()
        return results
    end
    return self:continue(r, conn, data.token, options)
end

function M:continue(r, conn, token, options)
    local data, err = r.protocol:continue_query(r, conn, token)
    if not data then
        self:close()
        return nil, err
    end
    return self:process(r, conn, data, options)
end

function M:parse(r, response, options)
    local t = response.t

    local err = error_types[t]
    if err then
        return nil, false, response.r[1] .. " / " .. r.json.encode(response.b)
    end

    if basic_responses[t] then
        response.r, err = convert_pseudo_type(r, response.r, options)
        if not response.r then
            return nil, false, err
        end

        if t == Response.SUCCESS_PARTIAL then
            return true, true, response.r
        end

        return true, false, response.r
    end

    if t == Response.WAIT_COMPLETE then
        return true, false
    end

    return nil, false, "Unknown response type from server [" .. t .. "]"
end

function M:close()
    self.status = "ready"
    self.results = {}
end

return M