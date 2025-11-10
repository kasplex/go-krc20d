
function init()

	local ds = mpz.new(session.block.daaScore, 10)
	if ds:cmp(110165000)<0 then
		return krc20.fail("out of range")
	end

	local sp = session.opParams
	local to = sp.to
	if to==nil or to=="" then
		to = sp.from
	end

	return krc20.succ({
		opParams = {
			tick = sp.ca,
			to = to,
		},
		opRules = {
			tick = "txid,r",
			amt = "amt,r",
			to = "addr,o",
		},
		keyRules = {
			[krc20.keyToken(sp.ca)] = "w",
			[krc20.keyBalance(sp.ca,to)] = "w",
		},
	})

end

function run()

	local sp = session.opParams
	local keyToken = krc20.keyToken(sp.tick)
	local stToken = state[keyToken]

	if stToken==nil then
		return krc20.fail("tick not found")
	elseif stToken.mod~="issue" then
		return krc20.fail("mode invalid")
	elseif sp.from~=stToken.to then
		return krc20.fail("no ownership")
	elseif sp.to==nil then
		return krc20.fail("address invalid")
	end

	local amt = mpz.new(sp.amt, 10)
	local minted = mpz.new(stToken.minted, 10)
	if stToken.max~="0" then
		local left = mpz.new(stToken.max,10):sub(minted)
		if left:cmp(0)<=0 then
			return krc20.fail("issue finished")
		end
		if amt:cmp(left)>0 then
			amt = left
		end
	end
	local amtString = tostring(amt)
	minted:add(amt)

	local keyTo = krc20.keyBalance(sp.tick, sp.to)
	local stBalanceTo = state[keyTo]
	if stBalanceTo==nil then
		stBalanceTo = {
			address = sp.to,
			tick = sp.tick,
			dec = stToken.dec,
			balance = "0",
			locked = "0",
			opmod = "0",
		}
	end
	amt:add(mpz.new(stBalanceTo.balance,10))

	stBalanceTo.balance = tostring(amt)
	stBalanceTo.opmod = session.op.score
	stToken.minted = tostring(minted)
	stToken.opmod = session.op.score
	stToken.mtsmod = session.block.timestamp

	return krc20.succ({
		opParams = {
			name = stToken.name,
			amt = amtString,
		},
		state = {
			{keyToken, stToken},
			{keyTo, stBalanceTo},
		},
	})

end
