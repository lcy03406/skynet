#define LUA_LIB

#include <lua.h>
#include <lauxlib.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>

#include "skynet.h"

/*
	uint32_t/string addr
	uint32_t/session session
	lightuserdata msg
	uint32_t sz

	return
		string request
		uint32_t next_session
 */

static void
fill_uint32(uint8_t * buf, uint32_t n) {
	buf[0] = n & 0xff;
	buf[1] = (n >> 8) & 0xff;
	buf[2] = (n >> 16) & 0xff;
	buf[3] = (n >> 24) & 0xff;
}

static void
fill_header(lua_State *L, uint8_t *buf, int sz) {
	assert(sz < 0x500000);
	buf[0] = (sz >> 24) & 0xff;
	buf[1] = (sz >> 16) & 0xff;
	buf[2] = (sz >> 8) & 0xff;
	buf[3] = sz & 0xff;
}

/*
	The request package :
		first DWORD is size of the package with big-endian
		DWORD in content is small-endian

	address is id
		DWORD sz+10+nodelen
		BYTE 0
		BYTE nodelen
		STRING node
		DWORD addr
		DWORD session
		PADDING msg(sz)
	address is string
		DWORD sz+7+nodelen+namelen
		BYTE 1
		BYTE nodelen
		STRING node
		BYTE namelen
		STRING name
		DWORD session
		PADDING msg(sz)
 */
static int
packreq_number(lua_State *L, const char* node, size_t nodelen, int session, void * msg, uint32_t sz, int is_push) {
	uint32_t addr = (uint32_t)lua_tointeger(L,2);
	uint8_t* buf = (uint8_t*)skynet_malloc(4+sz+10+nodelen);
	int pos = 0;
	fill_header(L, buf, sz+10+nodelen);
	pos += 4;
	buf[pos] = 0;
	pos += 1;
	buf[pos] = (uint8_t)nodelen;
	pos += 1;
	memcpy(buf+pos, node, nodelen);
	pos += nodelen;
	fill_uint32(buf+pos, addr);
	pos += 4;
	fill_uint32(buf+pos, is_push ? 0 : (uint32_t)session);
	pos += 4;
	memcpy(buf+pos,msg,sz);

	lua_pushlstring(L, (const char *)buf, pos+sz);
	skynet_free(buf);
	return 0;
}

static int
packreq_string(lua_State *L, const char* node, size_t nodelen, int session, void * msg, uint32_t sz, int is_push) {
	size_t namelen = 0;
	const char *name = lua_tolstring(L, 2, &namelen);
	if (name == NULL || namelen < 1 || namelen > 255) {
		skynet_free(msg);
		luaL_error(L, "name is too long %s", name);
	}
	uint8_t* buf = (uint8_t*)skynet_malloc(4+sz+7+nodelen+namelen);
	int pos = 0;
	fill_header(L, buf, sz+7+namelen+nodelen);
	pos += 4;
	buf[pos] = 1;
	pos += 1;
	buf[pos] = (uint8_t)nodelen;
	pos += 1;
	memcpy(buf+pos, node, nodelen);
	pos += nodelen;
	buf[pos] = (uint8_t)namelen;
	pos += 1;
	memcpy(buf+pos, name, namelen);
	pos += namelen;
	fill_uint32(buf+pos, is_push ? 0 : (uint32_t)session);
	pos += 4;
	memcpy(buf+pos,msg,sz);

	lua_pushlstring(L, (const char *)buf, pos+sz);
	skynet_free(buf);
	return 0;
}

static int
packrequest(lua_State *L, int is_push) {
	void *msg = lua_touserdata(L,4);
	if (msg == NULL) {
		return luaL_error(L, "Invalid request message");
	}
	uint32_t sz = (uint32_t)luaL_checkinteger(L,5);
	int session = luaL_checkinteger(L,3);
	if (session <= 0) {
		skynet_free(msg);
		return luaL_error(L, "Invalid request session %d", session);
	}
	size_t nodelen = 0;
	const char *node = lua_tolstring(L, 1, &nodelen);
	if (node == NULL || nodelen < 1 || nodelen > 255) {
		skynet_free(msg);
		return luaL_error(L, "node name is too long %s", node);
	}
	int addr_type = lua_type(L,2);
	if (addr_type == LUA_TNUMBER) {
		packreq_number(L, node, nodelen, session, msg, sz, is_push);
	} else {
		packreq_string(L, node, nodelen, session, msg, sz, is_push);
	}
	uint32_t new_session = (uint32_t)session + 1;
	if (new_session > INT32_MAX) {
		new_session = 1;
	}
	lua_pushinteger(L, new_session);
	//free msg
	int dont = lua_toboolean(L,6);
	if(dont == 0) {
		skynet_free(msg);
	}
	return 2;
}

static int
lpack(lua_State *L) {
	int is_push = lua_toboolean(L, -1);
	return packrequest(L, is_push);
}

static int
lpackrequest(lua_State *L) {
	return packrequest(L, 0);
}

static int
lpackpush(lua_State *L) {
	return packrequest(L, 1);
}

/*
	string packed message
	return
	string node
	uint32_t or string addr
	int session
	lightuserdata msg
	int sz
	boolean is_push
 */

static inline uint32_t
unpack_uint32(const uint8_t * buf) {
	return buf[0] | buf[1]<<8 | buf[2]<<16 | buf[3]<<24;
}

static void
return_buffer(lua_State *L, const char * buffer, int sz) {
	void * ptr = skynet_malloc(sz);
	memcpy(ptr, buffer, sz);
	lua_pushlightuserdata(L, ptr);
	lua_pushinteger(L, sz);
}

static int
unpackreq_number(lua_State *L, const uint8_t * buf, int sz) {
	if (sz < 2) {
		return luaL_error(L, "Invalid proxy message (size=%d)", sz);
	}
	int pos = 1;
	size_t nodesz = buf[pos];
	pos += 1;
	if (sz < nodesz + pos + 8) {
		return luaL_error(L, "Invalid proxy message (size=%d)", sz);
	}
	lua_pushlstring(L, (const char *)buf+pos, nodesz);
	pos += nodesz;
	uint32_t address = unpack_uint32(buf+pos);
	pos += 4;
	uint32_t session = unpack_uint32(buf+pos);
	pos += 4;
	lua_pushinteger(L, address);
	lua_pushinteger(L, session);

	return_buffer(L, (const char *)buf+pos, sz-pos);
	if (session == 0) {
		lua_pushboolean(L,1);	// is_push, no reponse
		return 6;
	}

	return 5;
}

static int
unpackreq_string(lua_State *L, const uint8_t * buf, int sz) {
	if (sz < 2) {
		return luaL_error(L, "Invalid proxy message (size=%d)", sz);
	}
	int pos = 1;
	size_t nodesz = buf[pos];
	pos += 1;
	if (sz < nodesz + pos) {
		return luaL_error(L, "Invalid proxy message (size=%d)", sz);
	}
	lua_pushlstring(L, (const char *)buf+pos, nodesz);
	pos += nodesz;
	size_t namesz = buf[pos];
	pos += 1;
	if (sz < namesz + pos + 4) {
		return luaL_error(L, "Invalid proxy message (size=%d)", sz);
	}
	lua_pushlstring(L, (const char *)buf+pos, namesz);
	pos += namesz;
	uint32_t session = unpack_uint32(buf + pos);
	lua_pushinteger(L, (uint32_t)session);
	pos += 4;
	return_buffer(L, (const char *)buf+pos, sz-pos);
	if (session == 0) {
		lua_pushboolean(L,1);	// is_push, no reponse
		return 6;
	}
	return 5;
}

static int
unpacktrace(lua_State *L, const char * buf, int sz) {
	lua_pushlstring(L, buf + 1, sz - 1);
	return 1;
}

static int
lunpackrequest(lua_State *L) {
	int sz;
	const char *msg;
	if (lua_type(L, 1) == LUA_TLIGHTUSERDATA) {
		msg = (const char *)lua_touserdata(L, 1);
		sz = luaL_checkinteger(L, 2);
	} else {
		size_t ssz;
		msg = luaL_checklstring(L,1,&ssz);
		sz = (int)ssz;
	}
	switch (msg[0]) {
		case 0:
			return unpackreq_number(L, (const uint8_t *)msg, sz);
		case 1:
			return unpackreq_string(L, (const uint8_t *)msg, sz);
		case 2:
			return unpacktrace(L, msg, sz);
		default:
			return luaL_error(L, "Invalid req package type %d", msg[0]);
	}
}

/*
	The response package :
	DWORD size (big endian)
	BYTE  3
	DWORD session
	BYTE type
		0: error
		1: ok
*/

/*
	int session
	boolean ok
	lightuserdata msg
	int sz
	return string response
 */
static int
lpackresponse(lua_State *L) {
	uint32_t session = (uint32_t)luaL_checkinteger(L,1);
	int ok = lua_toboolean(L,2);

	void * msg;
	size_t sz;
	if (lua_type(L,3) == LUA_TSTRING) {
		msg = (void *)lua_tolstring(L, 3, &sz);
	} else {
		msg = lua_touserdata(L,3);
		sz = (size_t)luaL_checkinteger(L, 4);
	}

	uint8_t* buf = (uint8_t*)skynet_malloc(sz+10);
	int pos = 0;
	fill_header(L, buf, sz+6);
	pos += 4;
	buf[pos] = 3;
	pos += 1;
	fill_uint32(buf+pos, session);
	pos += 4;
	buf[pos] = ok;
	pos += 1;
	memcpy(buf+pos,msg,sz);

	lua_pushlstring(L, (const char *)buf, sz+pos);
	skynet_free(buf);
	return 1;
}

/*
	string packed response
	return
		integer session
		boolean ok
		string msg
*/
static int
lunpackresponse(lua_State *L) {
	int sz;
	const char *msg;
	if (lua_type(L, 1) == LUA_TLIGHTUSERDATA) {
		msg = (const char *)lua_touserdata(L, 1);
		sz = luaL_checkinteger(L, 2);
	} else {
		size_t ssz;
		msg = luaL_checklstring(L,1,&ssz);
		sz = (int)ssz;
	}
	if (sz < 6) {
		return luaL_error(L, "Invalid response message (size=%d)", sz);
	}
	int pos = 1;
	uint32_t session = unpack_uint32((const uint8_t*)msg+pos);
	lua_pushinteger(L, (lua_Integer)session);
	pos += 4;
	switch(msg[pos++]) {
		case 0:	// error
			lua_pushboolean(L, 0);
			lua_pushlstring(L, msg+pos, sz-pos);
			return 3;
		case 1:	// ok
			lua_pushboolean(L, 1);
			lua_pushlstring(L, msg+pos, sz-pos);
			return 3;
		default:
			return 1;
	}
}

static int
lunpack(lua_State *L) {
	int sz;
	const char *msg;
	int msg_type = lua_type(L,1);
	if (LUA_TLIGHTUSERDATA == msg_type) {
		msg = (const char *)lua_touserdata(L, 1);
		sz = luaL_checkinteger(L, 2);
	} else {
		size_t ssz;
		msg = luaL_checklstring(L,1,&ssz);
		sz = (int)ssz;
	}
	int cnt = 1;
	lua_pushboolean(L, msg[0] == 3 ? 1 : 0);
	switch (msg[0]) {
		case 0:
		case 1:
		case 2:
			cnt += lunpackrequest(L);
			break;
		case 3:
			cnt += lunpackresponse(L);
			break;
		default:
			return luaL_error(L, "Invalid req package type %d", msg[0]);
	}
	if (LUA_TLIGHTUSERDATA == msg_type) {
		skynet_free((void*)msg);
	}
	return cnt;
}

LUAMOD_API int
luaopen_skynet_proxy_core(lua_State *L) {
	luaL_Reg l[] = {
		{ "pack", lpack },
		{ "packrequest", lpackrequest },
		{ "packpush", lpackpush },
		{ "packresponse", lpackresponse },
		{ "unpack", lunpack },
		{ NULL, NULL },
	};
	luaL_checkversion(L);
	luaL_newlib(L,l);

	return 1;
}
