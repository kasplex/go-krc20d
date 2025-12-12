
function init()

	local ds = mpz.new(session.block.daaScore, 10)
	if ds:cmp(110165000)<0 then
		return krc20.fail("out of range")
	end

	local sp = session.opParams

	if sp.mod~="add" or sp.mod~="remove" then
		return krc20.fail("mode invalid")
	end

	return krc20.succ({
		opParams = {
			tick = sp.ca,
		},
		opRules = {
			tick = "txid,r",
			to = "addr,o",
		},
		keyRules = {
			[krc20.keyToken(sp.ca)] = "r",
			[krc20.keyBlacklist(sp.ca,to)] = "w",
		},
	})

end

function run()

	local sp = session.opParams
	local stToken = state[krc20.keyToken(sp.tick)]

	if stToken==nil then
		return krc20.fail("tick not found")
	elseif stToken.mod~="issue" then
		return krc20.fail("mode invalid")
	elseif sp.from~=stToken.to then
		return krc20.fail("no ownership")
	elseif sp.to==nil then
		return krc20.fail("address invalid")
	end

	local keyBlacklist = krc20.keyBlacklist(sp.tick, sp.to)
	local stBlacklist = state[keyBlacklist]

	if sp.mod=="add" then
		if stBlacklist~=nil then
			return krc20.fail("no affected")
		end
		stBlacklist = {
			tick = sp.tick,
			address = sp.to,
			opadd = session.op.score,
		}
	elseif sp.mod=="remove" then
		if stBlacklist==nil then
			return krc20.fail("no affected")
		end
		stBlacklist = {}
	end

	return krc20.succ({
		opParams = {name=stToken.name},
		state = {
			{keyBlacklist, stBlacklist},
		},
	})

end
