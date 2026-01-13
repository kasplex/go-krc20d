
function init()

	local ds = mpz.new(session.block.daaScore, 10)
	if ds:cmp(83441551)<0 or ds:cmp(83525600)>0 and ds:cmp(90090600)<0 then
		return krc20.fail("out of range")
	end

	local sp = session.opParams
	local to = sp.to
	if to==nil or to=="" then
		to = sp.from
	end

	return krc20.succ({
		feeLeast = "100000000",
		opParams = {
			to = to,
		},
		opRules = {
			tick = "tick,r",
			to = "addr,o",
		},
		keyRules = {
			[krc20.keyToken(sp.tick)] = "w",
			[krc20.keyBalance(sp.tick,to)] = "w",
		},
	})

end

function run()

	local sp = session.opParams
	local keyToken = krc20.keyToken(sp.tick)
	local stToken = state[keyToken]

	if stToken==nil then
		return krc20.fail("tick not found")
	elseif stToken.mod~=nil and stToken.mod~="" and stToken.mod~="mint" then
		return krc20.fail("mode invalid")
	elseif session.tx.fee==nil or session.tx.fee=="0" then
		return krc20.fail("fee unknown")
	end

	local fee = mpz.new(session.tx.fee, 10)
	local feeLeast = mpz.new(session.op.feeLeast, 10)
	if fee:cmp(feeLeast)<0 then
		return krc20.fail("fee not enough")
	end

	if sp.to==nil then
		return krc20.fail("address invalid")
	end

	local minted = mpz.new(stToken.minted, 10)
	local left = mpz.new(stToken.max,10):sub(minted)
	local lim = mpz.new(stToken.lim, 10)

	if left:cmp(0)<=0 then
		return krc20.fail("mint finished")
	end
	if lim:cmp(left)>0 then
		lim = left
	end
	local amtString = tostring(lim)
	minted:add(lim)

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
	lim:add(mpz.new(stBalanceTo.balance,10))

	stBalanceTo.balance = tostring(lim)
	stBalanceTo.opmod = session.op.score
	stToken.minted = tostring(minted)
	stToken.opmod = session.op.score
	stToken.mtsmod = session.block.timestamp

	return krc20.succ({
		opParams = {amt=amtString},
		state = {
			{keyToken, stToken},
			{keyTo, stBalanceTo},
		},
	})

end
