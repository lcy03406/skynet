local skynet = require "skynet"
local sd = require "skynet.sharedata.corelib"

local service

skynet.init(function()
	service = skynet.uniqueservice "sharedatad"
end)

local sharedata = {}
local cache = setmetatable({}, { __mode = "kv" })
local watching = {}

local function monitor(name, obj, cobj)
	local newcobj = cobj
	while true do
		newcobj = skynet.call(service, "lua", "monitor", name, newcobj)
		if newcobj == nil then
			break
		end
		sd.update(obj, newcobj)
		local watch = watching[name]
		if watch then 
			local r = sd.box(newcobj)
			watch(name, r)
		end
	end
	if cache[name] == obj then
		cache[name] = nil
	end
	local watch = watching[name]
	if watch then 
		watch(name, nil)
	end
end

function sharedata.query(name)
	if cache[name] then
		return cache[name]
	end
	local obj = skynet.call(service, "lua", "query", name)
	if obj == nil then
		return nil
	end
	local r = sd.box(obj)
	skynet.send(service, "lua", "confirm" , obj)
	skynet.fork(monitor, name, r, obj)
	cache[name] = r
	return r
end

function sharedata.watch(name, watch)
	watching[name] = watch
end

function sharedata.new(name, v, ...)
	skynet.call(service, "lua", "new", name, v, ...)
end

function sharedata.update(name, v, ...)
	skynet.call(service, "lua", "update", name, v, ...)
end

function sharedata.delete(name)
	skynet.call(service, "lua", "delete", name)
end

function sharedata.flush()
	for name, obj in pairs(cache) do
		sd.flush(obj)
	end
	collectgarbage()
end

function sharedata.deepcopy(name, ...)
	if cache[name] then
		local cobj = cache[name].__obj
		return sd.copy(cobj, ...)
	end

	local cobj = skynet.call(service, "lua", "query", name)
	local ret = sd.copy(cobj, ...)
	skynet.send(service, "lua", "confirm" , cobj)
	return ret
end


return sharedata
