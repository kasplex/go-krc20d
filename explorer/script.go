
////////////////////////////////
package explorer

import (
    "fmt"
    "time"
    "sync"
    "strconv"
    "strings"
    "unicode"
    "math/big"
    "encoding/hex"
    "github.com/kasplex/go-lyncs"
    "kasplex-executor/misc"
    "kasplex-executor/storage"
    "kasplex-executor/sequencer"
)

////////////////////////////////
var scriptKeyForcedCompatibleList = map[string]bool{"from":true,"to":true,"tick":true,"max":true,"lim":true,"pre":true,"dec":true,"amt":true,"utxo":true,"price":true,"mod":true,"name":true,"ca":true}

////////////////////////////////
// Parse the P2SH transaction input script.
func parseScriptInput(script string) (bool, []string) {
    script = strings.ToLower(script)
    lenScript := len(script)
    if (lenScript <= 138) {
        return false, nil
    }
    // Get the next data length and position.
    _lGet := func(s string, i int) (int64, int, bool) {
        iRaw := i
        lenS := len(s)
        if lenS < (i + 2) {
            return 0, iRaw, false
        }
        f := s[i:i+2]
        i += 2
        lenD := int64(0)
        if f == "4c" {
            if lenS < (i + 2) {
                return 0, iRaw, false
            }
            f := s[i:i+2]
            i += 2
            lenD, _ = strconv.ParseInt(f, 16, 32)
        } else if f == "4d" {
            if lenS < (i + 4) {
                return 0, iRaw, false
            }
            f := s[i+2:i+4] + s[i:i+2]
            i += 4
            lenD, _ = strconv.ParseInt(f, 16, 32)
        } else {
            lenD, _ = strconv.ParseInt(f, 16, 32)
            if (lenD <0 || lenD > 75) {
                return 0, iRaw, false
            }
        }
        lenD *= 2
        return lenD, i, true
    }
    
    // Get the push number and position.
    _nGet := func(s string, i int) (int64, int, bool) {
        iRaw := i
        lenS := len(s)
        if lenS < (i + 2) {
            return 0, iRaw, false
        }
        f := s[i:i+2]
        i += 2
        num, _ := strconv.ParseInt(f, 16, 32)
        if (num < 81 || num > 96) {
            return 0, iRaw, false
        }
        num -= 80
        return num, i, true
    }
    
    // Get the last data position.
    _dGotoLast := func(s string, i int) (int, bool) {
        iRaw := i
        lenS := len(s)
        lenD := int64(0)
        r := true
        for j := 0; j < 16; j ++ {
            lenD, i, r = _lGet(s, i)
            if !r {
                return iRaw, false
            }
            if lenS < (i + int(lenD)) {
                return iRaw, false
            } else if lenS == (i + int(lenD)) {
                if lenD < 94 {
                    return iRaw, false
                }
                return i, true
            } else {
                i += int(lenD)
            }
        }
        return iRaw, false
    }
    
    // Skip to the redeem script.
    r := true
    n := 0
    flag := ""
    n, r = _dGotoLast(script, n)
    if !r {
        return false, nil
    }
    
    // Get the public key or multisig script hash
    scriptSig := ""
    multisig := false
    mm := int64(0)
    nn := int64(0)
    kPub := ""
    lenD := int64(0)
    mm, n, r = _nGet(script, n)
    if r {
        if (mm > 0 && mm < 16) {
            multisig = true
        } else {
            return false, nil
        }
    }
    if !multisig {
        lenD, n, r = _lGet(script, n)
        if !r {
            return false, nil
        }
        fSig := ""
        if lenScript > (n + int(lenD) + 2) {
            fSig = script[n+int(lenD):n+int(lenD)+2]
        }
        if (lenD == 64 && fSig == "ac") {
            kPub = script[n:n+64]
            n += 66
            scriptSig = "20" + kPub + fSig
        } else if (lenD == 66 && fSig == "ab") {
            kPub = script[n:n+66]
            n += 68
            scriptSig = "21" + kPub + fSig
        } else {
            return false, nil
        }
    } else {
        var kPubList []string
        for j := 0; j < 16; j ++ {
            lenD, n, r = _lGet(script, n)
            if !r {
                nn, n, r = _nGet(script, n)
                if (!r || len(kPubList) != int(nn)) {
                    return false, nil
                }
                kPub, scriptSig = misc.ConvKPubListToScriptHashMultisig(mm, kPubList, nn)
                break
            }
            if (lenD == 64 || lenD == 66) {
                kPubList = append(kPubList, script[n:n+int(lenD)])
                n += int(lenD)
            } else {
                return false, nil
            }
        }
        if lenScript < (n + 2) {
            return false, nil
        }
        flag = script[n:n+2]
        n += 2
        if (flag != "a9" && flag != "ae") {
            return false, nil
        }
    }
    if kPub == "" {
        return false, nil
    }
    // Check the protocol header.
    if lenScript < (n + 22) {
        return false, nil
    }
    flag = script[n:n+6]
    n += 6
    if flag != "006307" {
        return false, nil
    }
    flag = script[n:n+14]
    n += 14
    decoded, _ := hex.DecodeString(flag)
    header := strings.ToUpper(string(decoded[:]))
    if header != "KASPLEX" {
        return false, nil
    }
    
    // Get the next param data and position.
    _pGet := func(s string, i int) (string, int, bool) {
        iRaw := i
        lenS := len(s)
        lenP := int64(0)
        lenP, i, r = _lGet(s, i)
        if (!r || lenS < (i + int(lenP))) {
            return "", iRaw, false
        }
        if lenP == 0 {
            return "", i, true
        }
        decoded, _ = hex.DecodeString(s[i:i+int(lenP)])
        p := string(decoded[:])
        i += int(lenP)
        return p, i, true
    }
    
    // Get the param and json data.
    p0 := ""
    p1 := ""
    p2 := ""
    r = true
    for j := 0; j < 2; j ++ {
        if lenScript < (n + 2) {
            return false, nil
        }
        flag = script[n:n+2]
        n += 2
        if flag == "00" {
            p0, n, r = _pGet(script, n)
        } else if flag == "68" {
            break
        } else {
            if flag == "51" {
                p1 = "p1"
            } else if flag == "53" {
                p1 = "p3"
            } else if flag == "55" {
                p1 = "p5"
            } else if flag == "57" {
                p1 = "p7"
            } else if flag == "59" {
                p1 = "p9"
            } else if flag == "5b" {
                p1 = "p11"
            } else if flag == "5d" {
                p1 = "p13"
            } else if flag == "5f" {
                p1 = "p15"
            } else {
                return false, nil
            }
            p2, n, r = _pGet(script, n)
        }
        if !r {
            return false, nil
        }
    }
    if p0 == "" {
        return false, nil
    }
    
    // Get the from address.
    from := ""
    if multisig {
        from = misc.ConvKPubToP2sh(kPub, eRuntime.testnet)
    } else {
        from = misc.ConvKPubToAddr(kPub, eRuntime.testnet)
    }
    return true, []string{from, p0, p1, p2, scriptSig}
}

////////////////////////////////
// Parse the OP data in transaction.
func parseOpData(txData *storage.DataTransactionType) ([]lyncs.DataCallFuncType) {
    if (txData == nil || txData.Data == nil) {
        return nil
    }
    lenInput := len(txData.Data.Inputs)
    if lenInput <= 0 {
        return nil
    }
    txInputs := make([]map[string]string, 0, len(txData.Data.Inputs))
    txOutputs := make([]map[string]string, 0, len(txData.Data.Outputs))
    for _, input := range txData.Data.Inputs {
        txInputs = append(txInputs, map[string]string{
            "prevTxId": input.PreviousOutpoint.TransactionId,
            "prevIndex": strconv.FormatUint(uint64(input.PreviousOutpoint.Index),10),
        })
    }
    for _, output := range txData.Data.Outputs {
        txOutputs = append(txOutputs, map[string]string{
            "amount": strconv.FormatUint(uint64(output.Amount),10),
            "spk": output.ScriptPublicKey.ScriptPublicKey,
            "type": output.VerboseData.ScriptPublicKeyType,
            "address": output.VerboseData.ScriptPublicKeyAddress,
        })
    }
    callList := make([]lyncs.DataCallFuncType, 0, 4)
    for i, input := range txData.Data.Inputs {
        scriptSig := ""
        isOp, scriptInfo := parseScriptInput(input.SignatureScript)
        if (!isOp || scriptInfo[0] == "") {
            continue
        }
        decoded := make(map[string]string, 32)
        decodedRaw := make(map[string]interface{}, 32)
        err := json.Unmarshal([]byte(scriptInfo[1]), &decodedRaw)
        if err != nil {
            continue
        }
        var ok bool
        ignored := false
        for k, v := range decodedRaw {
            decoded[k], ok = v.(string)
            if !ok {
                delete(decoded, k)
                if scriptKeyForcedCompatibleList[k] {
                    ignored = true
                    break
                }
            }
        }
        if ignored {
            continue
        }
        decoded["from"] = scriptInfo[0]
        if (!eRuntime.testnet && txData.DaaScore <= 83525600 && len(txData.Data.Outputs) > 0) {  // use output[0]
            decoded["to"] = txData.Data.Outputs[0].VerboseData.ScriptPublicKeyAddress
        }
        if (!validateBy(validateP,decoded,"p") || !validateBy(validateOp,decoded,"op") || (decoded["to"]!="" && !validateBy(validateAscii,decoded,"to"))) {
            continue
        }
        validateBy(validateTickTxId, decoded, "tick")
        validateBy(validateTxId, decoded, "ca")
        if i == 0 {
            scriptSig = scriptInfo[4]
        }
        testnet := ""
        if eRuntime.testnet {
            testnet = "1"
        }
        callList = append(callList, lyncs.DataCallFuncType{
            Name: decoded["p"] + "_" + decoded["op"],
            Fn: "init",
            Session: &lyncs.DataSessionType{
                Block: map[string]string{
                    "daaScore": strconv.FormatUint(txData.DaaScore,10),
                    "hash": txData.BlockAccept,
                    "timestamp": strconv.FormatUint(txData.BlockTime,10),
                },
                Tx: map[string]string{
                    "id": txData.TxId,
                    //"hash": txData.Data.VerboseData.Hash,
                    "fee": "0",
                },
                TxInputs: txInputs,
                TxOutputs: txOutputs,
                Op: map[string]string{
                    "index": strconv.Itoa(i),
                    "spkFrom": scriptSig,
                    "testnet": testnet,
                },
                OpParams: decoded,
            },
        })
    }
    return callList
}

////////////////////////////////
// Parse the OP data and prepare the state key in transaction list.
func ParseOpDataList(txDataList []storage.DataTransactionType) ([]storage.DataOperationType, storage.DataStateMapType, int64, error) {
    mtss := time.Now().UnixMilli()
    lenTx := len(txDataList)
    stateMap := make(storage.DataStateMapType)
    opDataMap := make(map[string]*storage.DataOperationType, lenTx)
    txIdMap := make(map[string]bool, lenTx)
    callInitList := make([]lyncs.DataCallFuncType, 0, lenTx*12/10)
    mutex := new(sync.RWMutex)
    misc.GoBatch(lenTx, func(i int, iBatch int) (error) {
        callList := parseOpData(&txDataList[i])
        if len(callList) <= 0 {
            return nil
        }
        mutex.Lock()
        callInitList = append(callInitList, callList...)
        mutex.Unlock()
        return nil
    })
    if len(callInitList) <= 0 {
        return nil, nil, 0, nil
    }

fmt.Println("lenCallInit = ", len(callInitList))
mts1 := time.Now().UnixMilli()
fmt.Println("mts = ", mts1)
fmt.Println("lenTx = ", lenTx)
    
    resultList := lyncs.CallFuncParallel(callInitList, storage.DataStateMapType{}, nil, nil,
        // Process result use hook.
        func(c *lyncs.DataCallFuncType, i int, r *lyncs.DataResultType, err error) (*lyncs.DataResultType) {
            if err != nil || r == nil {
                
if err != nil {
    fmt.Println("CallFuncParallel error: ", err)
}
                
                return nil
            }
            // Check if OP recycle.
            r.Op["spkFrom"] = c.Session.Op["spkFrom"]
            r.Op["feeLeast"] = r.Op["feeLeast"]
            validateBy(validateAmount, r.Op, "feeLeast")
            if c.Session.Op["index"] != "0" && r.Op["isRecycle"] != "1" {
                return nil
            }
            delete(r.Op, "isRecycle")

            // Check state key.
            validateStateKey(r.KeyRules)
            if len(r.KeyRules) <= 0 {
                return nil
            }
            // Check and update OpParams with OpRules.
            for k, v := range r.OpRules {
                rule := strings.Split(strings.ReplaceAll(v," ",""), ",")
                ignored := false
                required := true
                if len(rule) > 1 && rule[1] == "o" {
                    required = false
                }
                target, existed := r.OpParams[k]
                if !existed  {
                    target, existed = c.Session.OpParams[k]
                    if !existed && required {
                        target = "_nil"
                    }
                }
                pass := true
                if len(rule) > 0 {
                    if target == "_nil" {
                        pass = false
                    } else if k == "tick" && rule[0] != "tick" && rule[0] != "txid" && rule[0] != "ticktxid" {
                        pass = false
                    } else if rule[0] == "tick" {
                        pass = validateTick(&target)
                    } else if rule[0] == "txid" {
                        pass = validateTxId(&target)
                    } else if rule[0] == "ticktxid" {
                        pass = validateTickTxId(&target)
                    } else if rule[0] == "addr" {
                        pass = validateAddr(&target)
                    } else if rule[0] == "amt" {
                        pass = validateAmount(&target)
                    } else if rule[0] == "dec" {
                        pass = validateDec(&target)
                    } else if rule[0] == "ascii" {
                        pass = validateAscii(&target)
                    } else {
                        ignored = true
                    }
                } else {
                    ignored = true
                }
                if !pass && required {
                    return nil
                }
                if !ignored && target != "" {
                    r.OpParams[k] = target
                } else {
                    delete(r.OpParams, k)
                }
            }
            r.OpParams["p"] = c.Session.OpParams["p"]
            r.OpParams["op"] = c.Session.OpParams["op"]
            r.OpParams["from"] = c.Session.OpParams["from"]
            // Return and update result.
            return r
        },
    )
    
mts2 := time.Now().UnixMilli()
fmt.Println("mts = ", mts2)
fmt.Println("lenResult1 = ", len(resultList))
lenResult := 0

    for i, r := range resultList {
        if r == nil {
            continue
        }
        
lenResult ++
        
        session := callInitList[i].Session
        txId := session.Tx["id"]
        for k := range r.KeyRules {
            stateMap[k] = nil
            if strings.HasPrefix(k, storage.KeyPrefixStateStats) {
                delete(r.KeyRules, k)
            }
        }
        if opDataMap[txId] == nil {
            opDataMap[txId] = &storage.DataOperationType{
                Block: session.Block,
                Tx: session.Tx,
                TxInputs: session.TxInputs,
                TxOutputs: session.TxOutputs,
                Op: r.Op,
                OpScript: make([]map[string]string, 0, 8),
                OpIndex: make([]int, 0, 2),
                OpKeyRules: make([]map[string]string, 0, 8),
                StBefore: make([]string, 0, 8),
                StAfter: make([]string, 0, 8),
            }
        }
        index, _ := strconv.Atoi(session.Op["index"])
        opDataMap[txId].OpScript = append(opDataMap[txId].OpScript, r.OpParams)
        opDataMap[txId].OpIndex = append(opDataMap[txId].OpIndex, index)
        opDataMap[txId].OpKeyRules = append(opDataMap[txId].OpKeyRules, r.KeyRules)
        if r.Op["feeLeast"] != "0" {
            for _, input := range session.TxInputs {
                txIdMap[input["prevTxId"]] = true
            }
        }
    }
    
fmt.Println("lenResult2 = ", lenResult)

    if len(opDataMap) <= 0 {
        return nil, nil, 0, nil
    }
    
fmt.Println("lenOpDataMap = ", len(opDataMap))

    txDataListInput := make([]storage.DataTransactionType, 0, len(txIdMap))
    for txId := range txIdMap {
        txDataListInput = append(txDataListInput, storage.DataTransactionType{TxId: txId})
    }
    txDataMapInput, _, err := sequencer.GetTxDataMap(txDataListInput)
    if err != nil {
        return nil, nil, 0, err
    }
    opDataList := []storage.DataOperationType{}
    daaScoreNow := uint64(0)
    opScore := uint64(0)
    for _, txData := range txDataList {
        if opDataMap[txData.TxId] == nil {
            continue
        }
        if daaScoreNow != txData.DaaScore {
            daaScoreNow = txData.DaaScore
            opScore = daaScoreNow * 10000
        }
        opDataMap[txData.TxId].Op["score"] = strconv.FormatUint(opScore, 10)
        if opDataMap[txData.TxId].Op["feeLeast"] != "0" {
            amountIn := uint64(0)
            amountOut := uint64(0)
            for _, output := range txData.Data.Outputs {
                amountOut += output.Amount
            }
            for _, input := range txData.Data.Inputs {
                if input.VerboseData != nil && input.VerboseData.UtxoEntry != nil && input.VerboseData.UtxoEntry.Amount > 0 {
                    amountIn += input.VerboseData.UtxoEntry.Amount
                    continue
                }
                if txDataMapInput[input.PreviousOutpoint.TransactionId] == nil {
                    continue
                }
                amountIn += txDataMapInput[input.PreviousOutpoint.TransactionId].Outputs[input.PreviousOutpoint.Index].Amount
            }
            if amountIn <= amountOut {
                opDataMap[txData.TxId].Tx["fee"] = "0"
                continue
            }
            opDataMap[txData.TxId].Tx["fee"] = strconv.FormatUint(amountIn-amountOut, 10)
        }
        opDataList = append(opDataList, *opDataMap[txData.TxId])
        opScore ++
    }
    return opDataList, stateMap, time.Now().UnixMilli()-mtss, nil
}

////////////////////////////////
func validateStateKey(keyRules map[string]string) (bool) {
    if len(keyRules) == 0 {
        return false
    }
    result := true
    for k, r := range keyRules {
        keys := strings.SplitN(k, "_", 2)
        if len(keys) < 2 || !storage.KeyPrefixStateMap[keys[0]] || r != "w" && r != "r" {
            delete(keyRules, k)
            result = false
            continue
        }
        if keys[0]==storage.KeyPrefixStateToken {
            keyRules[storage.KeyPrefixStateStats+"_"+keys[1]] = "r"
        }
    }
    keyRules[storage.KeyPrefixStateStats+"_#KRC-20"] = "r"
    return result
}

////////////////////////////////
func validateBy(fn func(*string) (bool), dataMap map[string]string, key string) (bool) {
    v, exists := dataMap[key]
    if !exists {
        return false
    }
    r := fn(&v)
    dataMap[key] = v
    return r
}

////////////////////////////////
func validateAscii(s *string) (bool) {
    if *s == "" {
        return true
    }
    for _, c := range *s {
        if c > unicode.MaxASCII {
            *s = ""
            return false
        }
    }
    return true
}

////////////////////////////////
func validateP(p *string) (bool) {
    *p = strings.ToUpper(*p)
    if *p != "KRC-20" {
        *p = ""
        return false
    }
    return true
}

////////////////////////////////
func validateOp(op *string) (bool) {
    *op = strings.ToLower(*op)
    if len(*op) <= 0 || len(*op) > 16 {
        *op = ""
        return false
    }
    if !validateAscii(op) {
        return false
    }
    return true
}

////////////////////////////////
func validateTick(tick *string) (bool) {
    *tick = strings.ToUpper(*tick)
    lenTick := len(*tick)
    if (lenTick < 4 || lenTick > 6) {
        *tick = ""
        return false
    }
    for i := 0; i < lenTick; i++ {
        if ((*tick)[i] < 65 || (*tick)[i] > 90) {
            *tick = ""
            return false
        }
    }
    return true
}

////////////////////////////////
func validateTxId(txid *string) (bool) {
    *txid = strings.ToLower(*txid)
    if len(*txid) != 64 {
        *txid = ""
        return false
    }
    _, err := hex.DecodeString(*txid)
    if err != nil {
        *txid = ""
        return false
    }
    return true
}

////////////////////////////////
func validateTickTxId(tick *string) (bool) {
    if len(*tick) != 64 {
        return validateTick(tick)
    }
    return validateTxId(tick)
}

////////////////////////////////
func validateAddr(addr *string) (bool) {
    if !validateAscii(addr) {
        return false
    }
    if !misc.VerifyAddr(*addr, eRuntime.testnet) {
        *addr = ""
        return false
    }
    return true
}

////////////////////////////////
func validateAmount(amount *string) (bool) {
    if *amount == "" {
        *amount = "0"
        return false
    }
    amountBig := new(big.Int)
    _, s := amountBig.SetString(*amount, 10)
    if !s {
        *amount = "0"
        return false
    }
    amount2 := amountBig.Text(10)
    if *amount != amount2 {
        *amount = "0"
        return false
    }
    limitBig := new(big.Int)
    limitBig.SetString("0", 10)
    if limitBig.Cmp(amountBig) >= 0 {
        *amount = "0"
        return false
    }
    limitBig.SetString("99999999999999999999999999999999", 10)
    if amountBig.Cmp(limitBig) > 0 {
        *amount = "99999999999999999999999999999999"
        return false
    }
    return true
}

////////////////////////////////
func validateDec(dec *string) (bool) {
    if *dec == "" {
        *dec = "8"
        return false
    }
    decInt, err := strconv.Atoi(*dec)
    if err != nil {
        *dec = "8"
        return false
    }
    decString := strconv.Itoa(decInt)
    if (decString != *dec || decInt < 0 || decInt > 18) {
        *dec = "8"
        return false
    }
    return true
}

// ...

