
function init()

	local ds = mpz.new(session.block.daaScore, 10)
	if ds:cmp(110165000)<0 then
		return krc20.fail("out of range")
	end

	local sp = session.opParams
	local tick = sp.tick
	local opr = {
		tick = "ticktxid,r",
		amt = "amt,r",
	}
	if sp.ca~=nil then
		tick = sp.ca
	end

	return krc20.succ({
		opParams = {
			tick = tick,
		},
		opRules = opr,
		keyRules = {
			[krc20.keyToken(tick)] = "w",
			[krc20.keyBalance(tick,sp.from)] = "w",
		},
	})

end

function run()

	local sp = session.opParams
	local keyToken = krc20.keyToken(sp.tick)
	local stToken = state[keyToken]

	if stToken==nil then
		return krc20.fail("tick not found")
	elseif sp.from~=stToken.to then
		return krc20.fail("no ownership")
	end

	local keyFrom = krc20.keyBalance(sp.tick, sp.from)
	local stBlanceFrom = state[keyFrom]

	if stBlanceFrom==nil then
		return krc20.fail("balance insuff")
	end

	local amt = mpz.new(sp.amt, 10)
	local amtFrom = mpz.new(stBlanceFrom.balance, 10)
	if amt:cmp(amtFrom)>0 then
		return krc20.fail("balance insuff")
	end
	
	stBlanceFrom.balance = tostring(amtFrom:sub(amt))
	stBlanceFrom.opmod = session.op.score
	stToken.burned = tostring(amt:add(mpz.new(stToken.burned,10)))
	stToken.opmod = session.op.score
	stToken.mtsmod = session.block.timestamp

	return krc20.succ({
		opParams = {name=stToken.name},
		state = {
			{keyToken, stToken},
			{keyFrom, stBlanceFrom},
		},
	})

end
