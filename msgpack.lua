--[[

	MessagePack encoder / decoder written in pure Lua 5.3 / Lua 5.4
	written by Sebastian Steinhauer <s.steinhauer@yahoo.de>

	This is free and unencumbered software released into the public domain.

	Anyone is free to copy, modify, publish, use, compile, sell, or
	distribute this software, either in source code form or as a compiled
	binary, for any purpose, commercial or non-commercial, and by any
	means.

	In jurisdictions that recognize copyright laws, the author or authors
	of this software dedicate any and all copyright interest in the
	software to the public domain. We make this dedication for the benefit
	of the public at large and to the detriment of our heirs and
	successors. We intend this dedication to be an overt act of
	relinquishment in perpetuity of all present and future rights to this
	software under copyright law.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
	EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
	MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
	IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
	OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
	ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
	OTHER DEALINGS IN THE SOFTWARE.

	For more information, please refer to <http://unlicense.org/>

--]]

local type, pcall, pairs, select = type, pcall, pairs, select;
local tconcat, tunpack = table.concat, table.unpack;
local pack, unpack = string.pack, string.unpack;
local mtype, utf8len = math.type, utf8.len;
local ssub = string.sub;
local ttype = type(table.type) == 'function' and table.type or function(t)
    if (type(t) ~= 'table') then
        return nil;
    end

    if (next(t) ~= nil and #t == 0) then
        return 'hash';
    end

    return 'array';
end;

msgpackl = {};

local encoder_functions, decoder_functions = {}, {};
local registered_extensions = {};

---@param value any
---@return string
local function encode_value(value)
    return encoder_functions[type(value)](value);
end

---@param data any
---@param position integer
---@return number, integer
local function decode_value(data, position)
    local byte, value;
    byte, position = unpack('B', data, position);
    value, position = decoder_functions[byte](data, position);
    return value, position;
end

---@param data any
---@param position integer
---@param length integer
---@return table, integer
local function decode_array(data, position, length)
    local elements, value = {};
    for i = 1, length do
        value, position = decode_value(data, position)
        elements[i] = value;
    end
    return elements, position;
end

---@param data any
---@param position integer
---@param length integer
---@return table, integer
local function decode_map(data, position, length)
    local elements, key, value = {};
    for i = 1, length do
        key, position = decode_value(data, position);
        value, position = decode_value(data, position);
        elements[key] = value;
    end
    return elements, position;
end

---@param extension number
local function extend(extension)
    assert(type(extension) == 'table', 'Extension must be a table');
    assert(type(extension.__ext) == 'number', 'Extension must have a __ext field with a number value');
    assert(not registered_extensions[extension.__ext], 'Extension with this __ext already registered');
    local serialize_type = type(extension.__serialize);
    assert(serialize_type == 'function' or serialize_type == 'table',
        'Extension must have a __serialize function or table');
    local deserialize_type = type(extension.__deserialize);
    assert(deserialize_type == 'function' or deserialize_type == 'table',
        'Extension must have a __deserialize function or table');
    registered_extensions[extension.__ext] = extension;
end

---@param extensionId number
local function extend_clear(extensionId)
    assert(type(extensionId) == 'number', 'Extension ID must be a number');
    assert(registered_extensions[extensionId], 'No extension registered with this ID');
    registered_extensions[extensionId] = nil;
end

---@param extensionId number
---@return table?
local function extend_get(extensionId)
    assert(type(extensionId) == 'number', 'Extension ID must be a number');
    return registered_extensions[extensionId];
end

---@return string
encoder_functions['nil'] = function()
    return pack('B', 0xc0);
end

---@param value boolean
---@return string
encoder_functions['boolean'] = function(value)
    return pack('B', value == true and 0xc3 or 0xc2);
end

---@param value number
---@return string
encoder_functions['number'] = function(value)
    if (mtype(value) == 'integer') then
        if (value >= 0) then
            if (value < 128) then
                return pack('B', value);
            elseif (value <= 0xff) then
                return pack('BB', 0xcc, value);
            elseif (value <= 0xffff) then
                return pack('>BI2', 0xcd, value);
            elseif (value <= 0xffffffff) then
                return pack('>BI4', 0xce, value);
            end
            return pack('>BI8', 0xcf, value);
        else
            if (value >= -32) then
                return pack('B', 0xe0 + (value + 32));
            elseif (value >= -128) then
                return pack('Bb', 0xd0, value);
            elseif (value >= -32768) then
                return pack('>Bi2', 0xd1, value);
            elseif (value >= -2147483648) then
                return pack('>Bi4', 0xd2, value);
            end
            return pack('>Bi8', 0xd3, value);
        end
    else
        local test = unpack('f', pack('f', value))
        if (test == value) then -- check if we can use float
            return pack('>Bf', 0xca, value);
        else
            return pack('>Bd', 0xcb, value);
        end
    end
end

---@param value string
---@return string
encoder_functions['string'] = function(value)
    local len = #value;
    if (utf8len(value)) then -- check if it is a real utf8 string or just byte junk
        if (len < 32) then
            return pack('B', 0xa0 + len) .. value;
        elseif (len < 256) then
            return pack('>Bs1', 0xd9, value);
        elseif (len < 65536) then
            return pack('>Bs2', 0xda, value);
        end
        return pack('>Bs4', 0xdb, value);
    else -- encode it as byte-junk
        if (len < 256) then
            return pack('>Bs1', 0xc4, value);
        elseif (len < 65536) then
            return pack('>Bs2', 0xc5, value);
        end
        return pack('>Bs4', 0xc6, value);
    end
end

---@param value table
---@return string
encoder_functions['table'] = function(value)
    local mt = getmetatable(value);

    if (mt and mt.__ext) then
        local ext_id = mt.__ext;
        local ext = registered_extensions[ext_id];
        assert(ext, "No extension registered for id " .. tostring(ext_id));
        local payload = encode_value(ext.__serialize(value, ext_id));
        local payload_len = #payload;

        if (payload_len == 1) then
            return pack('BBb', 0xd4, ext_id) .. payload;
        elseif (payload_len == 2) then
            return pack('BBb', 0xd5, ext_id) .. payload;
        elseif (payload_len == 4) then
            return pack('BBb', 0xd6, ext_id) .. payload;
        elseif (payload_len == 8) then
            return pack('BBb', 0xd7, ext_id) .. payload;
        elseif (payload_len == 16) then
            return pack('BBb', 0xd8, ext_id) .. payload;
        elseif (payload_len < 256) then
            return pack('BBb', 0xc7, payload_len, ext_id) .. payload;
        elseif (payload_len < 65536) then
            return pack('>BI2b', 0xc8, payload_len, ext_id) .. payload;
        end
        return pack('>BI4b', 0xc9, payload_len, ext_id) .. payload;
    end

    if (ttype(value) == 'array') then -- it seems to be a proper Lua array
        local elements = {};
        for i = 1, #value do
            elements[i] = encode_value(value[i]);
        end

        local length = #elements;
        if (length < 16) then
            return pack('>B', 0x90 + length) .. tconcat(elements);
        elseif (length < 65536) then
            return pack('>BI2', 0xdc, length) .. tconcat(elements);
        end
        return pack('>BI4', 0xdd, length) .. tconcat(elements);
    else -- encode as hash-map
        local elements = {};

        for k, v in pairs(value) do
            elements[#elements + 1] = encode_value(k);
            elements[#elements + 1] = encode_value(v);
        end

        local length = #elements // 2;

        if (length < 16) then
            return pack('>B', 0x80 + length) .. tconcat(elements);
        elseif (length < 65536) then
            return pack('>BI2', 0xde, length) .. tconcat(elements);
        end
        return pack('>BI4', 0xdf, length) .. tconcat(elements);
    end
end

---@param data any
---@param position integer
---@return any, integer
decoder_functions[0xc0] = function(data, position)
    return nil, position;
end

---@param data any
---@param position integer
---@return boolean, integer
decoder_functions[0xc2] = function(data, position)
    return false, position;
end

---@param data any
---@param position integer
---@return boolean, integer
decoder_functions[0xc3] = function(data, position)
    return true, position;
end

---@param data any
---@param position integer
---@return number, integer
decoder_functions[0xc4] = function(data, position)
    return unpack('>s1', data, position);
end

---@param data any
---@param position integer
---@return string, integer
decoder_functions[0xc5] = function(data, position)
    return unpack('>s2', data, position);
end

---@param data any
---@param position integer
---@return string, integer
decoder_functions[0xc6] = function(data, position)
    return unpack('>s4', data, position);
end

---@param data any
---@param position integer
---@return number, integer
decoder_functions[0xca] = function(data, position)
    return unpack('>f', data, position);
end

---@param data any
---@param position integer
---@return number, integer
decoder_functions[0xcb] = function(data, position)
    return unpack('>d', data, position);
end

---@param data any
---@param position integer
---@return number, integer
decoder_functions[0xcc] = function(data, position)
    return unpack('>B', data, position);
end

---@param data any
---@param position integer
---@return number, integer
decoder_functions[0xcd] = function(data, position)
    return unpack('>I2', data, position);
end

---@param data any
---@param position integer
---@return number, integer
decoder_functions[0xce] = function(data, position)
    return unpack('>I4', data, position);
end

---@param data any
---@param position integer
---@return number, integer
decoder_functions[0xcf] = function(data, position)
    return unpack('>I8', data, position);
end

---@param data any
---@param position integer
---@return number, integer
decoder_functions[0xd0] = function(data, position)
    return unpack('>b', data, position);
end

---@param data any
---@param position integer
---@return number, integer
decoder_functions[0xd1] = function(data, position)
    return unpack('>i2', data, position);
end

---@param data any
---@param position integer
---@return number, integer
decoder_functions[0xd2] = function(data, position)
    return unpack('>i4', data, position);
end

---@param data any
---@param position integer
---@return number, integer
decoder_functions[0xd3] = function(data, position)
    return unpack('>i8', data, position);
end

---@param data any
---@param position integer
---@return string, integer
decoder_functions[0xd9] = function(data, position)
    return unpack('>s1', data, position);
end

---@param data any
---@param position integer
---@return string, integer
decoder_functions[0xda] = function(data, position)
    return unpack('>s2', data, position);
end

---@param data any
---@param position integer
---@return string, integer
decoder_functions[0xdb] = function(data, position)
    return unpack('>s4', data, position);
end

---@param data any
---@param position integer
---@return number, integer
decoder_functions[0xdc] = function(data, position)
    local length, position = unpack('>I2', data, position);
    return decode_array(data, position, length);
end

---@param data any
---@param position integer
---@return number, integer
decoder_functions[0xdd] = function(data, position)
    local length, position = unpack('>I4', data, position);
    return decode_array(data, position, length);
end

---@param data any
---@param position integer
---@return table, integer
decoder_functions[0xde] = function(data, position)
    local length, position = unpack('>I2', data, position);
    return decode_map(data, position, length);
end

---@param data any
---@param position integer
---@return table, integer
decoder_functions[0xdf] = function(data, position)
    local length, position = unpack('>I4', data, position);
    return decode_map(data, position, length);
end

-- fixext 1
---@param data any
---@param position integer
---@return any, integer
decoder_functions[0xd4] = function(data, position)
    local ext_id, payload;
    ext_id, position = unpack('b', data, position);
    payload = ssub(data, position, position);
    position = position + 1;
    local ext = registered_extensions[ext_id];
    assert(ext, "No extension registered for id " .. tostring(ext_id));
    return ext.__deserialize(payload, ext_id), position;
end

-- fixext 2
---@param data any
---@param position integer
---@return any, integer
decoder_functions[0xd5] = function(data, position)
    local ext_id, payload;
    ext_id, position = unpack('b', data, position);
    payload = ssub(data, position, position + 1);
    position = position + 2;
    local ext = registered_extensions[ext_id];
    assert(ext, "No extension registered for id " .. tostring(ext_id));
    return ext.__deserialize(payload, ext_id), position;
end

-- fixext 4
---@param data any
---@param position integer
---@return any, integer
decoder_functions[0xd6] = function(data, position)
    local ext_id, payload;
    ext_id, position = unpack('b', data, position);
    payload = ssub(data, position, position + 3);
    position = position + 4;
    local ext = registered_extensions[ext_id];
    assert(ext, "No extension registered for id " .. tostring(ext_id));
    return ext.__deserialize(payload, ext_id), position;
end

-- fixext 8
---@param data any
---@param position integer
---@return any, integer
decoder_functions[0xd7] = function(data, position)
    local ext_id, payload;
    ext_id, position = unpack('b', data, position);
    payload = ssub(data, position, position + 7);
    position = position + 8;
    local ext = registered_extensions[ext_id];
    assert(ext, "No extension registered for id " .. tostring(ext_id));
    return ext.__deserialize(payload, ext_id), position;
end

-- fixext 16
---@param data any
---@param position integer
---@return any, integer
decoder_functions[0xd8] = function(data, position)
    local ext_id, payload;
    ext_id, position = unpack('b', data, position);
    payload = ssub(data, position, position + 15);
    position = position + 16;
    local ext = registered_extensions[ext_id];
    assert(ext, "No extension registered for id " .. tostring(ext_id));
    return ext.__deserialize(payload, ext_id), position;
end

-- ext8
---@param data any
---@param position integer
---@return any, integer
decoder_functions[0xc7] = function(data, position)
    local len, ext_id;
    len, position = unpack('B', data, position);
    ext_id, position = unpack('b', data, position);
    local payload = ssub(data, position, position + len - 1);
    position = position + len;
    local ext = registered_extensions[ext_id];
    assert(ext, "No extension registered for id " .. tostring(ext_id));
    return ext.__deserialize(payload, ext_id), position;
end

-- ext16
---@param data any
---@param position integer
---@return any, integer
decoder_functions[0xc8] = function(data, position)
    local len, ext_id;
    len, position = unpack('>I2', data, position);
    ext_id, position = unpack('b', data, position);
    local payload = ssub(data, position, position + len - 1);
    position = position + len;
    local ext = registered_extensions[ext_id];
    assert(ext, "No extension registered for id " .. tostring(ext_id));
    return ext.__deserialize(payload, ext_id), position;
end

-- ext32
---@param data any
---@param position integer
---@return any, integer
decoder_functions[0xc9] = function(data, position)
    local len, ext_id;
    len, position = unpack('>I4', data, position);
    ext_id, position = unpack('b', data, position);
    local payload = ssub(data, position, position + len - 1);
    position = position + len;
    local ext = registered_extensions[ext_id];
    assert(ext, "No extension registered for id " .. tostring(ext_id));
    return ext.__deserialize(payload, ext_id), position;
end

-- add fix-array, fix-map, fix-string, fix-int stuff
for i = 0x00, 0x7f do
    ---@param data any
    ---@param position integer
    ---@return number, integer
    decoder_functions[i] = function(data, position)
        return i, position;
    end
end

for i = 0x80, 0x8f do
    ---@param data any
    ---@param position integer
    ---@return table, integer
    decoder_functions[i] = function(data, position)
        return decode_map(data, position, i - 0x80);
    end
end

for i = 0x90, 0x9f do
    ---@param data any
    ---@param position integer
    ---@return table, integer
    decoder_functions[i] = function(data, position)
        return decode_array(data, position, i - 0x90);
    end
end

for i = 0xa0, 0xbf do
    ---@param data any
    ---@param position integer
    ---@return string, integer
    decoder_functions[i] = function(data, position)
        local length = i - 0xa0;
        return ssub(data, position, position + length - 1), position + length;
    end
end

for i = 0xe0, 0xff do
    ---@param data any
    ---@param position integer
    ---@return number, integer
    decoder_functions[i] = function(data, position)
        return -32 + (i - 0xe0), position;
    end
end

msgpackl._AUTHOR = 'Sebastian Steinhauer <s.steinhauer@yahoo.de>';
msgpackl._VERSION = '0.6.2';

---@vararg any
---@return string
function msgpackl.encode_many(...)
    local data, ok = {};
    for i = 1, select('#', ...) do
        ok, data[i] = pcall(encode_value, select(i, ...));
        if (not ok) then
            return nil, 'cannot encode MessagePack';
        end
    end
    return tconcat(data);
end

---@param value any
---@return string, string?
function msgpackl.encode(value)
    local ok, data = pcall(encode_value, value);
    if (ok) then
        return data;
    end
    return nil, 'cannot encode MessagePack';
end

---@param data string
---@param position integer?
---@return ... decoded_values
function msgpackl.decode_many(data, position)
    local values, value, ok = {};
    position = position or 1;
    while (position <= #data) do
        ok, value, position = pcall(decode_value, data, position);
        if (ok) then
            values[#values + 1] = value;
        else
            return nil, 'cannot decode MessagePack';
        end
    end
    return tunpack(values);
end

---@param data string
---@param position integer?
---@return any decoded_value
---@return integer position
function msgpackl.decode(data, position)
    local ok, value, position = pcall(decode_value, data, position or 1)
    if (ok) then
        return value, position;
    end
    return nil, 'cannot decode MessagePack';
end

msgpackl.extend = extend;
msgpackl.extend_clear = extend_clear;
msgpackl.extend_get = extend_get;

return msgpackl;
