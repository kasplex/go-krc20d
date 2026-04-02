
function init()

	local ds = mpz.new(session.block.daaScore, 10)
	if ds:cmp(83441551)<0 or ds:cmp(83525600)>0 and ds:cmp(90090600)<0 then
		return krc20.fail("out of range")
	end

	local sp = session.opParams
	if sp.to=="" then
		return krc20.fail("address invalid")
	end

	local tick = sp.tick
	local opr = {
		tick = "ticktxid,r",
		amt = "amt,r",
		to = "addr,o",
	}
	if sp.ca~=nil then
		tick = sp.ca
	end
	local opp = {tick=tick}
	
	if ds:cmp(408300500)>0 then
		local memo = ""
		if sp.memo~=nil then
			memo = string.sub(sp.memo, 1, 256)
		end
		opr.memo = "ascii,o"
		opp.memo = memo
	end

	return krc20.succ({
		opParams = opp,
		opRules = opr,
		keyRules = {
			[krc20.keyToken(tick)] = "r",
			[krc20.keyBlacklist(tick,sp.from)] = "r",
			[krc20.keyBalance(tick,sp.from)] = "w",
			[krc20.keyBalance(tick,sp.to)] = "w",
		},
	})

end

function run()

	local sp = session.opParams
	local stToken = state[krc20.keyToken(sp.tick)]
	local stBlacklist = state[krc20.keyBlacklist(sp.tick,sp.from)]

	if stToken==nil then
		return krc20.fail("tick not found")
	elseif stBlacklist~=nil then
		return krc20.fail("blacklist")
	elseif sp.to==nil or sp.from==sp.to then
		return krc20.fail("address invalid")
	end

	local keyFrom = krc20.keyBalance(sp.tick, sp.from)
	local stBlanceFrom = state[keyFrom]
	local keyTo = krc20.keyBalance(sp.tick, sp.to)
	local stBlanceTo = state[keyTo]

	if stBlanceFrom==nil then
		return krc20.fail("balance insuff")
	end

	if stBlanceTo==nil then
		stBlanceTo = {
			address = sp.to,
			tick = sp.tick,
			dec = stBlanceFrom.dec,
			balance = "0",
			locked = "0",
			opmod = "0",
		}
	end

	local amt = mpz.new(sp.amt, 10)
	local amtFrom = mpz.new(stBlanceFrom.balance, 10)
	if amt:cmp(amtFrom)>0 then
		return krc20.fail("balance insuff")
	end
	stBlanceFrom.balance = tostring(amtFrom:sub(amt))
	stBlanceFrom.opmod = session.op.score

	local amtTo = mpz.new(stBlanceTo.balance, 10)
	stBlanceTo.balance = tostring(amtTo:add(amt))
	stBlanceTo.opmod = session.op.score

	return krc20.succ({
		opParams = {name=stToken.name},
		state = {
			{keyFrom, stBlanceFrom},
			{keyTo, stBlanceTo},
		},
	})

end
