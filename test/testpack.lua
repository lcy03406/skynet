local skynet = require "skynet"
skynet.start(function()
	local t = {1,23,4,5,6}
	local msg,sz  = skynet.pack(t)
	skynet.unpack(msg, sz)
	skynet.trash(msg, sz)
end)
