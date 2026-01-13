
function init()

	local ds = mpz.new(session.block.daaScore, 10)
	if ds:cmp(97539090)<0 then
		return krc20.fail("out of range")
	end

	local sp = session.opParams
	local tick = sp.tick
	local opr = {
		tick = "ticktxid,r",
		to = "addr,r",
		utxo = "ascii,r",
		price = "amt,o",
	}
	if sp.ca~=nil then
		tick = sp.ca
	end

	local to = ""
	local utxo = session.txInputs[tonumber(session.op.index)+1].prevTxId.."_"..sp.from
	local price = ""
	if #session.txOutputs>0 then
		price = session.txOutputs[1].amount
	end
	if #session.txOutputs>1 and session.op.index=="0" then
		to = session.txOutputs[2].address
	else
		price = "0"
		to = sp.from
	end

	return krc20.succ({
		isRecycle = "1",
		opParams = {
			tick = tick,
			to = to,
			utxo = utxo,
			price = price,
		},
		opRules = opr,
		keyRules = {
			[krc20.keyToken(tick)] = "r",
			[krc20.keyBalance(tick,sp.from)] = "w",
			[krc20.keyBalance(tick,to)] = "w",
			[krc20.keyMarket(tick,sp.from,session.txInputs[tonumber(session.op.index)+1].prevTxId)] = "w",
		},
	})

end

function run()

	local sp = session.opParams
	local stToken = state[krc20.keyToken(sp.tick)]

	if stToken==nil then
		return krc20.fail("tick not found")
	end

	local uTxId, uAddr = string.match(sp.utxo, "([^_]+)_([^_]+)")
	local keyMarket = krc20.keyMarket(sp.tick, sp.from, uTxId)
	local stMarket = state[keyMarket]
	local keyFrom = krc20.keyBalance(sp.tick, sp.from)
	local stBlanceFrom = state[keyFrom]

	if stMarket==nil then
		return krc20.fail("order not found")
	elseif stBlanceFrom==nil then
		return krc20.fail("order abnormal")
	end

	local amtString = stMarket.tamt
	local amt = mpz.new(stMarket.tamt, 10)
	local locked = mpz.new(stBlanceFrom.locked, 10)

	if amt:cmp(locked)>0 then
		return krc20.fail("order abnormal")
	end

	local keyTo = krc20.keyBalance(sp.tick, sp.to)
	local stBlanceTo = state[keyTo]
	if stBlanceTo==nil or stBlanceTo.balance=="0" and stBlanceTo.locked=="0" then
		stBlanceTo = {
			address = sp.to,
			tick = sp.tick,
			dec = stBlanceFrom.dec,
			balance = "0",
			locked = "0",
			opmod = session.op.score,
		}
	end

	stBlanceFrom.locked = tostring(locked:sub(amt))
	stBlanceTo.balance = tostring(amt:add(mpz.new(stBlanceTo.balance,10)))
	-- opmod todo fix ..
	stMarket = {}

	return krc20.succ({
		opParams = {
			name = stToken.name,
			amt = amtString,
		},
		state = {
			{keyFrom, stBlanceFrom},
			{keyTo, stBlanceTo},
			{keyMarket, stMarket},
		},
	})

end