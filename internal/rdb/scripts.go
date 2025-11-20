package rdb

import "github.com/redis/go-redis/v9"

var enqueueScript = redis.NewScript(`
if redis.call("EXISTS", KEYS[1]) == 1 then
	return -1
end
if KEYS[4] ~= "" then
	local ok = redis.call("SET", KEYS[4], ARGV[1], "NX", "EX", ARGV[4])
	if not ok then
		return 0
	end
end
redis.call("SET", KEYS[1], ARGV[2])
if ARGV[5] == "now" then
	redis.call("LPUSH", KEYS[2], ARGV[1])
else
	redis.call("ZADD", KEYS[2], tonumber(ARGV[6]), ARGV[1])
end
redis.call("SADD", KEYS[3], ARGV[3])
return 1
`)

var dequeueScript = redis.NewScript(`
local id = redis.call("RPOPLPUSH", KEYS[1], KEYS[2])
if not id then
	return nil
end
redis.call("ZADD", KEYS[3], tonumber(ARGV[1]), id)
local data = redis.call("GET", ARGV[2] .. id)
return {id, data}
`)

var doneScript = redis.NewScript(`
redis.call("LREM", KEYS[1], 0, ARGV[1])
redis.call("ZREM", KEYS[2], ARGV[1])
if KEYS[4] ~= "" then
	redis.call("DEL", KEYS[4])
end
local retention = tonumber(ARGV[2])
if retention > 0 then
	redis.call("SET", KEYS[3], ARGV[3])
	redis.call("EXPIRE", KEYS[3], retention)
	redis.call("ZADD", KEYS[5], tonumber(ARGV[4]), ARGV[1])
else
	redis.call("DEL", KEYS[3])
end
return 1
`)

var retryScript = redis.NewScript(`
redis.call("LREM", KEYS[1], 0, ARGV[1])
redis.call("ZREM", KEYS[2], ARGV[1])
redis.call("SET", KEYS[4], ARGV[2])
redis.call("ZADD", KEYS[3], tonumber(ARGV[3]), ARGV[1])
return 1
`)

var archiveScript = redis.NewScript(`
redis.call("LREM", KEYS[1], 0, ARGV[1])
redis.call("ZREM", KEYS[2], ARGV[1])
redis.call("SET", KEYS[4], ARGV[2])
redis.call("ZADD", KEYS[3], tonumber(ARGV[3]), ARGV[1])
if KEYS[5] ~= "" then
	redis.call("DEL", KEYS[5])
end
return 1
`)

var forwardScript = redis.NewScript(`
local ids = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1], "LIMIT", 0, tonumber(ARGV[2]))
for _, id in ipairs(ids) do
	redis.call("ZREM", KEYS[1], id)
	redis.call("LPUSH", KEYS[2], id)
end
return #ids
`)

var recoverScript = redis.NewScript(`
local ids = redis.call("ZRANGEBYSCORE", KEYS[1], "-inf", ARGV[1], "LIMIT", 0, tonumber(ARGV[2]))
for _, id in ipairs(ids) do
	redis.call("ZREM", KEYS[1], id)
	redis.call("LREM", KEYS[2], 0, id)
	redis.call("LPUSH", KEYS[3], id)
end
return #ids
`)

var heartbeatScript = redis.NewScript(`
return redis.call("ZADD", KEYS[1], "XX", "CH", tonumber(ARGV[1]), ARGV[2])
`)
