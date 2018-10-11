local skynet = require "skynet"
local mongo = require "skynet.db.mongo"

local util = {}

function util.is_table_equal(t1,t2,ignore_mt)
	local ty1 = type(t1)
	local ty2 = type(t2)
	if ty1 ~= ty2 then return false end
	-- non-table types can be directly compared
	if ty1 ~= 'table' and ty2 ~= 'table' then return t1 == t2 end
	-- as well as tables which have the metamethod __eq
	local mt = getmetatable(t1)
	if not ignore_mt and mt and mt.__eq then return t1 == t2 end
	for k1,v1 in pairs(t1) do
		local v2 = t2[k1]
		if v2 == nil or not util.is_table_equal(v1,v2) then return false end
	end
	for k2,v2 in pairs(t2) do
		local v1 = t1[k2]
		if v1 == nil or not util.is_table_equal(v1,v2) then return false end
	end
	return true
end

function test()
	local db = mongo.client({host = "127.0.0.1"})
	local test = "test"
	db[test].testintkey:drop()
	local data1 = {
		_id = 1,
		backpack = {
			[40001] = {
				[123] = {
					count = 1,
					tid = 40001,
					index = 123,
				}
			},
			[40002] = {
				[124] = {
					count = 1,
					tid = 40002,
					index = 124,
				}
			}
		}
	}
	db[test].testintkey:safe_insert(data1)

	local data2 = {
		_id = 2,
		backpack = {
			[40001] = {
				count = 1,
				tid = 40001,
				index = 123,
			},
			[40002] = {
				count = 2,
				tid = 40002,
				index = 123,
			},
			[40003] = 3,
		}
	}
	db[test].testintkey:safe_insert(data2)

	local retdata1 = db[test].testintkey:findOne({_id = 1})
	assert(util.is_table_equal(data1, retdata1))
	print("data1 success")
	
	local retdata2 = db[test].testintkey:findOne({_id = 2})
	assert(util.is_table_equal(data2, retdata2))
	print("data2 success")
end

skynet.start(function()
	test()
end)
