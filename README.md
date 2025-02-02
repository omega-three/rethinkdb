#reql-lua

A Lua driver for RethinkDB.


## Installation

```bash
luarocks install reql-lua
```

## Usage

```Lua
local RethinkDB = require("reql-lua.rethinkdb")

local rethinkdb = RethinkDB:init()

local r = rethinkdb.reql

local data, err = r.db_list().run()
```
