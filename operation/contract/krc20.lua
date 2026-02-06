
krc20 = {

	fail = function(s)
		return {op={accept="-1",error=s}}
	end,
	succ = function(t)
		t.op = {accept="1"}
		t.op.feeLeast = t.feeLeast
		t.op.isRecycle = t.isRecycle
		t.feeLeast = nil
		t.isRecycle = nil
		local st = {}
		local k = ""
		for i,v in ipairs(t.state or {}) do
			st[v[1]] = v[2]
			k = k..v[1]..","
		end
		t.state = st
		t.exData = {keyList=k}
		return t
	end,

	keyToken = function(tick)
		return "sttoken_"..tick
	end,
	keyBalance = function(tick, addr)
		return "stbalance_"..addr.."_"..tick
	end,
	keyMarket = function(tick, addr, txid)
		return "stmarket_"..tick.."_"..addr.."_"..txid
	end,
	keyBlacklist = function(tick, addr)
		return "stblacklist_"..tick.."_"..addr
	end,
	
	tohex = function(s)
		return s:gsub('.',function(c) return string.format("%02x",string.byte(c)) end)
	end,
	fromhex = function(s)
		if #s%2~=0 or not s:match("^[%x]+$") then return "" end
		return s:gsub("%x%x",function(h) return string.char(tonumber(h,16)) end)
	end,

	makeScriptData = function(data)
		if #data<=0 or #data>65535 then return "\x00"
		elseif #data<=75 then return string.char(#data)..data
		elseif #data<=255 then return "\x4c"..string.char(#data)..data
		else return "\x4d"..string.char(bit.band(bit.rshift(#data,8),0xff))..string.char(bit.band(#data,0xff))..data end
	end,
	makeP2SH = function(spk, data)
		local s = krc20.fromhex(spk).."\x00\x63\x07\x6b\x61\x73\x70\x6c\x65\x78\x00"..krc20.makeScriptData(data).."\x68"
		local k = "\x08"..crypt.blake2b256(s)
		if session and session.op and session.op.testnet=="1" then return crypt.encbech32x(k,"kaspatest"), s
		else return crypt.encbech32x(k,"kaspa"), krc20.tohex(s) end
	end,

}