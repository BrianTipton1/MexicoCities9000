local key = KEYS[1];
local value = redis.call('GET', key);
local jvalue = cjson.encode(cmsgpack.unpack(value));
return jvalue;