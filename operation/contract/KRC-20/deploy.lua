
function init()

	local ds = mpz.new(session.block.daaScore, 10)
	if ds:cmp(83441551)<0 or ds:cmp(83525600)>0 and ds:cmp(90090600)<0 then
		return krc20.fail("out of range")
	end

	local sp = session.opParams
	local opp = {
		tick = sp.tick,
		lim = sp.lim,
		to = sp.to,
	}
	if sp.to==nil or sp.to=="" then
		opp.to = sp.from
	end

	local opr = {}
	if ds:cmp(110165000)>=0 and sp.mod=="issue" then
		opp.tick = session.tx.id
		opp.lim = "0"
		opr = {
			tick = "txid,r",
			name = "tick,r",
			mod = "ascii,r",
			lim = "amt,o",
		}
		if sp.max=="0" then
			opr.max = "amt,o"
		else
			opr.max = "amt,r"
		end
	else
		opr = {
			tick = "tick,r",
			max = "amt,r",
			lim = "amt,r",
		}
	end
	opr.dec = "dec,o"
	opr.pre = "amt,o"
	opr.to = "addr,o"

	return krc20.succ({
		feeLeast = "100000000000",
		opParams = opp,
		opRules = opr,
		keyRules = {
			[krc20.keyToken(opp.tick)] = "w",
			[krc20.keyBalance(opp.tick,opp.to)] = "w",
		},
	})

end

function run()

	local tickIgnored = {
		KASPA=true, KASPLX=true, KASP=true, WKAS=true, GIGA=true,
		WBTC=true, WETH=true, USDT=true, USDC=true, FDUSD=true,
		USDD=true, TUSD=true, USDP=true, PYUSD=true, EURC=true,
		BUSD=true, GUSD=true, EURT=true, XAUT=true, TETHER=true,
	}

	local tickReserved = {
		NACHO = "kaspa:qzrsq2mfj9sf7uye3u5q7juejzlr0axk5jz9fpg4vqe76erdyvxxze84k9nk7",
		KCATS = "kaspa:qq8guq855gxkfrj2w25skwgj7cp4hy08x6a8mz70tdtmgv5p2ngwqxpj4cknc",
		KASTOR = "kaspa:qr8vt54764aaddejhjfwtsh07jcjr49v38vrw2vtmxxtle7j2uepynwy57ufg",
		KASPER = "kaspa:qppklkx2zyr2g2djg3uy2y2tsufwsqjk36pt27vt2xfu8uqm24pskk4p7tq5n",
		FUSUN = "kaspa:qzp30gu5uty8jahu9lq5vtplw2ca8m2k7p45ez3y8jf9yrm5qdxquq5nl45t5",
		KPAW = "kaspa:qpp0y685frmnlvhmnz5t6qljatumqm9zmppwnhwu9vyyl6w8nt30qjedekmdw",
		PPKAS = "kaspa:qrlx9377yje3gvj9qxvwnn697d209lshgcrvge3yzlxnvyrfyk3q583jh3cmz",
		GHOAD = "kaspa:qpkty3ymqs67t0z3g7l457l79f9k6drl55uf2qeq5tlkrpf3zwh85es0xtaj9",
		KEPE = "kaspa:qq45gur2grn80uuegg9qgewl0wg2ahz5n4qm9246laej9533f8e22x3xe6hkm",
		WORI = "kaspa:qzhgepc7mjscszkteeqhy99d3v96ftpg2wyy6r85nd0kg9m8rfmusqpp7mxkq",
		KEKE = "kaspa:qqq9m42mdcvlz8c7r9kmpqj59wkfx3nppqte8ay20m4p46x3z0lsyzz34h8uf",
		DOGK = "kaspa:qpsj64nxtlwceq4e7jvrsrkl0y6dayfyrqr49pep7pd2tq2uzvk7ks7n0qwxc",
		BTAI = "kaspa:qp0na29g4lysnaep5pmg9xkdzcn4xm4a35ha5naq79ns9mcgc3pccnf225qma",
		KASBOT = "kaspa:qrrcpdaev9augqwy8jnnp20skplyswa7ezz3m9ex3ryxw22frpzpj2xx99scq",
		SOMPS = "kaspa:qry7xqy6s7d449gqyl0dkr99x6df0q5jlj6u52p84tfv6rddxjrucnn066237",
		KREP = "kaspa:qzaclsmr5vttzlt0rz0x3shnudny8lnz5zpmjr4lp9v7aa7u7zvexh05eqwq0",
	}

	local sp = session.opParams
	local keyToken = krc20.keyToken(sp.tick)
	local stToken = state[keyToken]

	if stToken~=nil then
		return krc20.fail("tick existed")
	elseif tickIgnored[sp.tick] then
		return krc20.fail("tick ignored")
	elseif tickReserved[sp.tick]~=nil and tickReserved[sp.tick]~=sp.from then
		return krc20.fail("tick reserved")
	elseif session.tx.fee==nil or session.tx.fee=="0" then
		return krc20.fail("fee unknown")
	end

	local fee = mpz.new(session.tx.fee, 10)
	local feeLeast = mpz.new(session.op.feeLeast, 10)
	if fee:cmp(feeLeast)<0 then
		return krc20.fail("fee not enough")
	end

	local pre = false
	if sp.pre~=nil and sp.pre~="0" then
		pre = true
	end
	if pre and sp.to==nil then
		return krc20.fail("address invalid")
	end

	local keyTo = krc20.keyBalance(sp.tick, sp.to)
	stToken = {
		tick = sp.tick,
		max = sp.max,
		lim = sp.lim,
		pre = sp.pre,
		dec = sp.dec,
		mod = sp.mod,
		from = sp.from,
		to = sp.to,
		minted = "0",
		burned = "0",
		txid = session.tx.id,
		opadd = session.op.score,
		opmod = session.op.score,
		mtsadd = session.block.timestamp,
		mtsmod = session.block.timestamp,
	}
	if sp.mod=="issue" then
		stToken.name = sp.name
	end

	local newState = {}
	if pre then
		local minted = mpz.new(sp.pre, 10)
		if sp.max~="0" then
			local tmax = mpz.new(sp.max, 10)
			if minted:cmp(tmax)>0 then
				minted = tmax
			end
		end
		stToken.minted = tostring(minted)
		local stBalanceTo = {
			address = sp.to,
			tick = sp.tick,
			dec = sp.dec,
			balance = stToken.minted,
			locked = "0",
			opmod = session.op.score,
		}
		newState = {
			{keyToken, stToken},
			{keyTo, stBalanceTo},
		}
	else
		newState = {
			{keyToken, stToken},
		}
	end

	return krc20.succ({state=newState})

end
