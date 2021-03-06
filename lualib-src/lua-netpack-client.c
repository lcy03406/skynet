#define LUA_LIB

#include "skynet_malloc.h"

#include "skynet_socket.h"

#include <lua.h>
#include <lauxlib.h>

#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#define QUEUESIZE 1024
#define HASHSIZE 4096
#define SMALLSTRING 2048

#define TYPE_DATA 1
#define TYPE_MORE 2
#define TYPE_ERROR 3
#define TYPE_OPEN 4
#define TYPE_CLOSE 5
#define TYPE_WARNING 6
#define TYPE_UDP 7
#define TYPE_UC 8

#ifdef CLIENT_USE_4_SIZE_BYTES
#define SIZE_BYTES 4
#else
#define SIZE_BYTES 2
#endif

/*
	Each package is uint16 + data , uint16 (serialized in big-endian) is the number of bytes comprising the data .
 */

struct netpack {
	int id;
	int size;
	void * buffer;
};

struct uncomplete {
	struct netpack pack;
	struct uncomplete * next;
	int read;
	int header_read;
	uint8_t header[SIZE_BYTES];
};

struct queue {
	int cap;
	int hashsize;
	int head;
	int tail;
	struct uncomplete** hash;
	struct netpack queue[0];
};

static void
clear_list(struct uncomplete * uc) {
	while (uc) {
		skynet_free(uc->pack.buffer);
		void * tmp = uc;
		uc = uc->next;
		skynet_free(tmp);
	}
}

static int
lclear(lua_State *L) {
	struct queue * q = lua_touserdata(L, 1);
	if (q == NULL) {
		return 0;
	}
	int i;
	for (i=0;i<q->hashsize;i++) {
		clear_list(q->hash[i]);
		q->hash[i] = NULL;
	}
	if (q->head > q->tail) {
		q->tail += q->cap;
	}
	for (i=q->head;i<q->tail;i++) {
		struct netpack *np = &q->queue[i % q->cap];
		skynet_free(np->buffer);
	}
	q->head = q->tail = 0;

	return 0;
}

static inline int
hash_fd(int fd, int hashsize) {
	int a = fd >> 24;
	int b = fd >> 12;
	int c = fd;
	return (int)(((uint32_t)(a + b + c)) % hashsize);
}

static struct uncomplete *
find_uncomplete(struct queue *q, int fd) {
	if (q == NULL)
		return NULL;
	int h = hash_fd(fd, q->hashsize);
	struct uncomplete * uc = q->hash[h];
	if (uc == NULL)
		return NULL;
	if (uc->pack.id == fd) {
		q->hash[h] = uc->next;
		return uc;
	}
	struct uncomplete * last = uc;
	while (last->next) {
		uc = last->next;
		if (uc->pack.id == fd) {
			last->next = uc->next;
			return uc;
		}
		last = uc;
	}
	return NULL;
}

static struct queue *
new_queue(lua_State *L, int hashsize, int qsize) {
	size_t sz = sizeof(struct queue) + qsize * sizeof(struct netpack) + hashsize * sizeof(struct uncomplete *);
	struct queue *q = lua_newuserdata(L, sz);
	q->cap = qsize;
	q->hashsize = hashsize;
	q->hash = (struct uncomplete**)((char*)q + sizeof(struct queue) + qsize * sizeof(struct netpack));
	q->head = 0;
	q->tail = 0;
	int i;
	for (i=0;i<hashsize;i++) {
		q->hash[i] = NULL;
	}
	return q;
}

static struct queue *
get_queue(lua_State *L) {
	struct queue *q = lua_touserdata(L,1);
	if (q == NULL) {
		q = new_queue(L, HASHSIZE, QUEUESIZE);
		lua_replace(L, 1);
	}
	return q;
}

static void
expand_queue(lua_State *L, struct queue *q) {
	int newcap = q->cap + (q->cap > QUEUESIZE ? QUEUESIZE : q->cap);
	struct queue *nq = new_queue(L, q->hashsize, newcap);
	nq->tail = q->cap;
	memcpy(nq->hash, q->hash, q->hashsize * sizeof(struct uncomplete *));
	memset(q->hash, 0, q->hashsize * sizeof(struct uncomplete *));
	int i;
	for (i=0;i<q->cap;i++) {
		int idx = (q->head + i) % q->cap;
		nq->queue[i] = q->queue[idx];
	}
	q->head = q->tail = 0;
	lua_replace(L,1);
}

static void
push_data(lua_State *L, int fd, void *buffer, int size, int clone) {
	if (clone) {
		void * tmp = skynet_malloc(size);
		memcpy(tmp, buffer, size);
		buffer = tmp;
	}
	struct queue *q = get_queue(L);
	struct netpack *np = &q->queue[q->tail];
	if (++q->tail >= q->cap)
		q->tail -= q->cap;
	np->id = fd;
	np->buffer = buffer;
	np->size = size;
	if (q->head == q->tail) {
		expand_queue(L, q);
	}
}

static void
push_uncomplete(struct queue *q, struct uncomplete *uc) {
	int h = hash_fd(uc->pack.id, q->hashsize);
	uc->next = q->hash[h];
	q->hash[h] = uc;
}

static struct uncomplete *
save_uncomplete(lua_State *L, int fd) {
	struct queue *q = get_queue(L);
	struct uncomplete * uc = skynet_malloc(sizeof(struct uncomplete));
	memset(uc, 0, sizeof(*uc));
	uc->pack.id = fd;
	push_uncomplete(q, uc);
	return uc;
}

static inline int
read_size(uint8_t * buffer) {
#ifdef CLIENT_USE_4_SIZE_BYTES
	int r = (int)buffer[0] << 24 |(int)buffer[1] << 16 | (int)buffer[2] << 8  | (int)buffer[3];
	if (r < 0 || r >= 0x1000000) { //16M
		return -1;
	}
#else
	int r = (int)buffer[0] << 8 | (int)buffer[1];
	if (r < 0 || r >= 0x10000) {
		return -1;
	}
#endif
	return r;
}

// 长度读入错误，返回错误
static int
push_more(lua_State *L, int fd, uint8_t *buffer, int size) {
	if (size < SIZE_BYTES) {
		struct uncomplete * uc = save_uncomplete(L, fd);
		uc->read = -1;
		memcpy(uc->header, buffer, size);
		uc->header_read = size;
		return 0;
	}
	int pack_size = read_size(buffer);
	if(pack_size == -1) {
		return -1;
	}
	buffer += SIZE_BYTES;
	size -= SIZE_BYTES;

	if (size < pack_size) {
		struct uncomplete * uc = save_uncomplete(L, fd);
		uc->read = size;
		uc->pack.size = pack_size;
		uc->pack.buffer = skynet_malloc(pack_size);
		memcpy(uc->pack.buffer, buffer, size);
		return 0;
	}
	push_data(L, fd, buffer, pack_size, 1);

	buffer += pack_size;
	size -= pack_size;
	if (size > 0) {
		return push_more(L, fd, buffer, size);
	}
	return 0;
}

static void
close_uncomplete(lua_State *L, int fd) {
	struct queue *q = lua_touserdata(L,1);
	struct uncomplete * uc = find_uncomplete(q, fd);
	if (uc) {
		skynet_free(uc->pack.buffer);
		skynet_free(uc);
	}
}

static int
filter_data_(lua_State *L, int fd, uint8_t * buffer, int size) {
	struct queue *q = lua_touserdata(L,1);
	struct uncomplete * uc = find_uncomplete(q, fd);
	if (uc) {
		// fill uncomplete
		if (uc->read < 0) {
			assert(uc->read == -1);
			if(uc->header_read+size<SIZE_BYTES)
			{
				memcpy(uc->header+uc->header_read, buffer, size);
				uc->header_read += size;
				push_uncomplete(q, uc);
				return 1;
			}
			// read size
			int need_bytes = SIZE_BYTES-uc->header_read;
			memcpy(uc->header+uc->header_read, buffer, need_bytes);

			int pack_size = read_size(uc->header);
			if(pack_size == -1) {
				close_uncomplete(L, fd);
				lua_pushvalue(L, lua_upvalueindex(TYPE_ERROR));
				lua_pushinteger(L, fd);
				lua_pushliteral(L, "read size");
				return 4;
			}

			buffer += need_bytes;
			size -= need_bytes;

			uc->pack.size = pack_size;
			uc->pack.buffer = skynet_malloc(pack_size);
			uc->read = 0;
			uc->header_read = 0;
		}
		int need = uc->pack.size - uc->read;
		if (size < need) {
			memcpy(uc->pack.buffer + uc->read, buffer, size);
			uc->read += size;
			push_uncomplete(q, uc);
			lua_pushvalue(L, lua_upvalueindex(TYPE_UC));
			lua_pushinteger(L, fd);
			lua_pushinteger(L, uc->pack.size);
			lua_pushinteger(L, uc->read);
			return 5;
		}
		memcpy(uc->pack.buffer + uc->read, buffer, need);
		buffer += need;
		size -= need;
		if (size == 0) {
			lua_pushvalue(L, lua_upvalueindex(TYPE_DATA));
			lua_pushinteger(L, fd);
			lua_pushlightuserdata(L, uc->pack.buffer);
			lua_pushinteger(L, uc->pack.size);
			skynet_free(uc);
			return 5;
		}
		// more data
		push_data(L, fd, uc->pack.buffer, uc->pack.size, 0);
		skynet_free(uc);
		if (push_more(L, fd, buffer, size) == -1) {
			close_uncomplete(L, fd);
			lua_pushvalue(L, lua_upvalueindex(TYPE_ERROR));
			lua_pushinteger(L, fd);
			lua_pushliteral(L, "read size");
			return 4;
		}
		lua_pushvalue(L, lua_upvalueindex(TYPE_MORE));
		return 2;
	} else {
		if (size < SIZE_BYTES) {
			struct uncomplete * uc = save_uncomplete(L, fd);
			uc->read = -1;
			memcpy(uc->header, buffer, size);
			uc->header_read = size;
			return 1;
		}
		int pack_size = read_size(buffer);
		if(pack_size == - 1) {
			close_uncomplete(L, fd);
			lua_pushvalue(L, lua_upvalueindex(TYPE_ERROR));
			lua_pushinteger(L, fd);
			lua_pushliteral(L, "read size");
			return 4;
		}

		buffer+=SIZE_BYTES;
		size-=SIZE_BYTES;

		if (size < pack_size) {
			struct uncomplete * uc = save_uncomplete(L, fd);
			uc->read = size;
			uc->pack.size = pack_size;
			uc->pack.buffer = skynet_malloc(pack_size);
			memcpy(uc->pack.buffer, buffer, size);
			lua_pushvalue(L, lua_upvalueindex(TYPE_UC));
			lua_pushinteger(L, fd);
			lua_pushinteger(L, pack_size);
			lua_pushinteger(L, size);
			return 5;
		}
		if (size == pack_size) {
			// just one package
			lua_pushvalue(L, lua_upvalueindex(TYPE_DATA));
			lua_pushinteger(L, fd);
			void * result = skynet_malloc(pack_size);
			memcpy(result, buffer, size);
			lua_pushlightuserdata(L, result);
			lua_pushinteger(L, size);
			return 5;
		}
		// more data
		push_data(L, fd, buffer, pack_size, 1);
		buffer += pack_size;
		size -= pack_size;
		if (push_more(L, fd, buffer, size) == -1) {
			close_uncomplete(L, fd);
			lua_pushvalue(L, lua_upvalueindex(TYPE_ERROR));
			lua_pushinteger(L, fd);
			lua_pushliteral(L, "read size");
			return 4;
		}
		lua_pushvalue(L, lua_upvalueindex(TYPE_MORE));
		return 2;
	}
}

static inline int
filter_data(lua_State *L, int fd, uint8_t * buffer, int size) {
	int ret = filter_data_(L, fd, buffer, size);
	// buffer is the data of socket message, it malloc at socket_server.c : function forward_message .
	// it should be free before return,
	skynet_free(buffer);
	return ret;
}

static void
pushstring(lua_State *L, const char * msg, int size) {
	if (msg) {
		lua_pushlstring(L, msg, size);
	} else {
		lua_pushliteral(L, "");
	}
}

/*
	userdata queue
	lightuserdata msg
	integer size
	return
		userdata queue
		integer type
		integer fd
		string msg | lightuserdata/integer
 */
static int
lfilter(lua_State *L) {
	struct skynet_socket_message *message = lua_touserdata(L,2);
	int size = luaL_checkinteger(L,3);
	char * buffer = message->buffer;
	if (buffer == NULL) {
		buffer = (char *)(message+1);
		size -= sizeof(*message);
	} else {
		size = -1;
	}

	lua_settop(L, 1);

	switch(message->type) {
	case SKYNET_SOCKET_TYPE_DATA:
		// ignore listen id (message->id)
		assert(size == -1);	// never padding string
		return filter_data(L, message->id, (uint8_t *)buffer, message->ud);
	case SKYNET_SOCKET_TYPE_CONNECT:
		// ignore listen fd connect
		return 1;
	case SKYNET_SOCKET_TYPE_CLOSE:
		// no more data in fd (message->id)
		close_uncomplete(L, message->id);
		lua_pushvalue(L, lua_upvalueindex(TYPE_CLOSE));
		lua_pushinteger(L, message->id);
		return 3;
	case SKYNET_SOCKET_TYPE_ACCEPT:
		lua_pushvalue(L, lua_upvalueindex(TYPE_OPEN));
		// ignore listen id (message->id);
		lua_pushinteger(L, message->ud);
		pushstring(L, buffer, size);
		return 4;
	case SKYNET_SOCKET_TYPE_ERROR:
		// no more data in fd (message->id)
		close_uncomplete(L, message->id);
		lua_pushvalue(L, lua_upvalueindex(TYPE_ERROR));
		lua_pushinteger(L, message->id);
		pushstring(L, buffer, size);
		return 4;
	case SKYNET_SOCKET_TYPE_WARNING:
		lua_pushvalue(L, lua_upvalueindex(TYPE_WARNING));
		lua_pushinteger(L, message->id);
		lua_pushinteger(L, message->ud);
		return 4;
	case SKYNET_SOCKET_TYPE_UDP:
		{
			lua_pushvalue(L, lua_upvalueindex(TYPE_UDP));
			lua_pushinteger(L, message->id);
			pushstring(L, buffer, message->ud);
			int addrsz = 0;
			const char * addrstring = skynet_socket_udp_address(message, &addrsz);
			pushstring(L, addrstring, addrsz);
			return 5;
		}
	default:
		// never get here
		return 1;
	}
}

/*
	userdata queue
	return
		integer fd
		lightuserdata msg
		integer size
 */
static int
lpop(lua_State *L) {
	struct queue * q = lua_touserdata(L, 1);
	if (q == NULL || q->head == q->tail)
		return 0;
	struct netpack *np = &q->queue[q->head];
	if (++q->head >= q->cap) {
		q->head = 0;
	}
	lua_pushinteger(L, np->id);
	lua_pushlightuserdata(L, np->buffer);
	lua_pushinteger(L, np->size);

	return 3;
}

/*
	string msg | lightuserdata/integer

	lightuserdata/integer
 */

static const char *
tolstring(lua_State *L, size_t *sz, int index) {
	const char * ptr;
	if (lua_isuserdata(L,index)) {
		ptr = (const char *)lua_touserdata(L,index);
		*sz = (size_t)luaL_checkinteger(L, index+1);
	} else {
		ptr = luaL_checklstring(L, index, sz);
	}
	return ptr;
}

static inline void
write_size(uint8_t * buffer, int len) {
#ifdef CLIENT_USE_4_SIZE_BYTES
	buffer[0] = (len >> 24) & 0xff;
	buffer[1] = (len >> 16) & 0xff;
	buffer[2] = (len >> 8) & 0xff;
	buffer[3] = len & 0xff;
#else
	buffer[0] = (len >> 8) & 0xff;
	buffer[1] = len & 0xff;
#endif
}

static int
lpack(lua_State *L) {
	size_t len;
	const char * ptr = tolstring(L, &len, 1);
#ifdef CLIENT_USE_4_SIZE_BYTES
	if (len >= 0x500000) {
#else
	if (len >= 0x10000) {
#endif
		return luaL_error(L, "Invalid size (too long) of data : %d", (int)len);
	}

	uint8_t * buffer = skynet_malloc(len + SIZE_BYTES);
	write_size(buffer, len);
	memcpy(buffer+SIZE_BYTES, ptr, len);

	lua_pushlightuserdata(L, buffer);
	lua_pushinteger(L, len + SIZE_BYTES);

	return 2;
}

static int
ltostring(lua_State *L) {
	void * ptr = lua_touserdata(L, 1);
	int size = luaL_checkinteger(L, 2);
	if (ptr == NULL) {
		lua_pushliteral(L, "");
	} else {
		lua_pushlstring(L, (const char *)ptr, size);
		skynet_free(ptr);
	}
	return 1;
}

static int
lnewqueue(lua_State *L) {
	int fdsize = luaL_checkinteger(L, 1);
	int qsize = luaL_checkinteger(L, 2);
	struct queue * q = new_queue(L, fdsize, qsize);
	(void)q;
	return 1;

}

LUAMOD_API int
luaopen_skynet_netpack_client(lua_State *L) {
	luaL_checkversion(L);
	luaL_Reg l[] = {
		{ "pop", lpop },
		{ "pack", lpack },
		{ "clear", lclear },
		{ "tostring", ltostring },
		{ "newqueue", lnewqueue },
		{ NULL, NULL },
	};
	luaL_newlib(L,l);

	// the order is same with macros : TYPE_* (defined top)
	lua_pushliteral(L, "data");
	lua_pushliteral(L, "more");
	lua_pushliteral(L, "error");
	lua_pushliteral(L, "open");
	lua_pushliteral(L, "close");
	lua_pushliteral(L, "warning");
	lua_pushliteral(L, "udp");
	lua_pushliteral(L, "uc");

	lua_pushcclosure(L, lfilter, 8);
	lua_setfield(L, -2, "filter");

	return 1;
}
