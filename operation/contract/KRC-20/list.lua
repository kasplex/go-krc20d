
function init()

	local ds = mpz.new(session.block.daaScore, 10)
	if ds:cmp(97539090)<0 then
		return krc20.fail("out of range")
	end

	local sp = session.opParams
	local tick = sp.tick
	local utxo = ""
	local opr = {
		tick = "tick,r",
		amt = "amt,r",
		utxo = "ascii,r",
	}
	if sp.ca~=nil then
		tick = sp.ca
		opr.tick = "txid,r"
	end

	if #session.txOutputs>0 then
		utxo = session.tx.id.."_"..session.txOutputs[0].address.."_"..session.txOutputs[0].amount
	end

	return krc20.succ({
		opParams = {
			tick = tick,
			utxo = utxo,
		},
		opRules = opr,
		keyRules = {
			[krc20.keyToken(tick)] = "r",
			[krc20.keyBalance(tick,sp.from)] = "w",
			[krc20.keyBlacklist(tick,sp.from)] = "r",
			[krc20.keyMarket(tick,sp.from,session.tx.id)] = "w",
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

	local tickLower = string.lower(sp.tick)
	local uTxId, uAddr, uAmt = string.match(sp.utxo, "([^_]+)_([^_]+)_([^_]+)")
	local uAddr1, uScript1 = krc20.makeP2SH(session.op.spkFrom, '{"p":"krc-20","op":"send","tick":"'..tickLower..'"}')
	local uAddr2, uScript2 = krc20.makeP2SH(session.op.spkFrom, '{"p":"krc-20","op":"send","ca":"'..tickLower..'"}')
	local uScript = ""
	
	if uAddr==uAddr1 then
		uScript = uScript1
	elseif uAddr==uAddr2 then
		uScript = uScript2
	else
		return krc20.fail("address invalid")
	end

	stBlanceFrom.balance = tostring(amtFrom:sub(amt))
	stBlanceFrom.locked = tostring(mpz.new(stBlanceFrom.locked,10):add(amt))
	stBlanceFrom.opmod = session.op.score

	local keyMarket = krc20.keyMarket(sp.tick, sp.from, uTxId)
	local stMarket = {
		tick = sp.tick,
		tamt = sp.amt,
		taddr = sp.from,
		utxid = uTxId,
		uaddr = uAddr,
		uamt = uAmt,
		uscript = uScript,
		opadd = session.op.score,
	}

	return krc20.succ({
		opParams = {name=stToken.name},
		state = {
			{keyFrom, stBlanceFrom},
			{keyMarket, stMarket},
		},
	})

end
