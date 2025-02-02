local M = {}

function M.bor(a, b)
    return a | b
end
function M.bxor(a, b)
    return a ~ b
end

return M