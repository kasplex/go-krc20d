
function init()

	local ds = mpz.new(session.block.daaScore, 10)
	if ds:cmp(110165000)<0 then
		return krc20.fail("out of range")
	end

	local sp = session.opParams
	local tick = sp.tick
	local opr = {
		tick = "tick,r",
		to = "addr,o",
	}
	if sp.ca~=nil then
		tick = sp.ca
		opr.tick = "txid,r"
	end

	return krc20.succ({
		opParams = {
			tick = tick,
		},
		opRules = opr,
		keyRules = {
			[krc20.keyToken(tick)] = "w",
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
	elseif sp.to==nil then
		return krc20.fail("address invalid")
	end

	stToken.to = sp.to
	stToken.opmod = session.op.score
	stToken.mtsmod = session.block.timestamp

	return krc20.succ({
		opParams = {name=stToken.name},
		state = {
			{keyToken, stToken},
		},
	})

end
