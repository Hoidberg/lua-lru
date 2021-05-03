-- lua-lru, LRU cache in Lua
-- Copyright (c) 2015 Boris Nagaev
-- See the LICENSE file for terms of use.

local lru = {}
lru.__index = lru

local cut
local setNewest
local del
local makeFreeSpace
local mynext

function lru.new(max_size, max_bytes)
	local self = setmetatable({}, lru)
	
	self._size = 0
	self._bytes_used = 0
	
	self._map = {}
	
	self._VALUE = 1
	self._PREV = 2
	self._NEXT = 3
	self._KEY = 4
	self._BYTES = 5
	
	self._newest = nil
	self._oldest = nil
	
	self._removed_tuple = nil
	
	cut = function(tuple)
		local tuple_prev = tuple[self._PREV]
		local tuple_next = tuple[self._NEXT]
		tuple[self._PREV] = nil
		tuple[self._NEXT] = nil
		if tuple_prev and tuple_next then
			tuple_prev[self._NEXT] = tuple_next
			tuple_next[self._PREV] = tuple_prev
		elseif tuple_prev then
			-- tuple is the oldest element
			tuple_prev[self._NEXT] = nil
			self._oldest = tuple_prev
		elseif tuple_next then
			-- tuple is the newest element
			tuple_next[self._PREV] = nil
			self._newest = tuple_next
		else
			-- tuple is the only element
			self._newest = nil
			self._oldest = nil
		end
	end
	
	setNewest = function(tuple)
		if not self._newest then
			self._newest = tuple
			self._oldest = tuple
		else
			tuple[self._NEXT] = self._newest
			self._newest[self._PREV] = tuple
			self._newest = tuple
		end
	end
	
	del = function(key, tuple)
		self._map[key] = nil
		cut(tuple)
		self._size = self._size - 1
		self._bytes_used = self._bytes_used - (tuple[self._BYTES] or 0)
		self._removed_tuple = tuple
	end
	
	makeFreeSpace = function(bytes)
		while self._size + 1 > max_size or
			(max_bytes and self._bytes_used + bytes > max_bytes)
		do
			assert(self._oldest, "not enough storage for cache")
			del(self._oldest[self._KEY], self._oldest)
		end
	end
	
	mynext = function(_, prev_key)
		local tuple
		if prev_key then
			tuple = self._map[prev_key][self._NEXT]
		else
			tuple = self._newest
		end
		if tuple then
			return tuple[self._KEY], tuple[self._VALUE]
		else
			return nil
		end
	end
	
	return self
end

function lru:__pairs()
	return mynext, nil, nil
end

function lru:get(_, key)
	local tuple = self._map[key]
	if not tuple then
		return nil
	end
	cut(tuple)
	setNewest(tuple)
	return tuple[self._VALUE]
end

function lru:set(_, key, value, bytes)
	local tuple = self._map[key]
	if tuple then
		del(key, tuple)
	end
	if value ~= nil then
		-- the value is not removed
		bytes = self._max_bytes and (bytes or #value) or 0
		makeFreeSpace(bytes)
		local tuple1 = self._removed_tuple or {}
		self._map[key] = tuple1
		tuple1[self._VALUE] = value
		tuple1[self._KEY] = key
		tuple1[self._BYTES] = self._max_bytes and bytes
		self._size = self._size + 1
		self._bytes_used = self._bytes_used + bytes
		setNewest(tuple1)
	else
		assert(key ~= nil, "Key may not be nil")
	end
	self._removed_tuple = nil
end

function lru:delete(_, key)
	return self:set(_, key, nil)
end

function lru:pairs()
	return mynext, nil, nil
end

return lru
