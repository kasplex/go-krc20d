
////////////////////////////////
package operation

import (
    "fmt"
    "log"
    "sync"
    "time"
    "embed"
    "io/fs"
    "strconv"
    "strings"
    "log/slog"
    "math/big"
    //"encoding/hex"
    "golang.org/x/crypto/blake2b"
    "github.com/kasplex/go-lyncs"
    "kasplex-executor/config"
    "kasplex-executor/misc"
    "kasplex-executor/storage"
)

//go:embed contract/krc20.lua
var builtinKrc20 string
//go:embed contract/KRC-20/*.lua
var fsGenesis embed.FS

////////////////////////////////
func InitLyncs(cfg config.LyncsConfig, stateContractMap map[string]*storage.StateContractType) {
    if cfg.NumSlot < 2 {
        cfg.NumSlot = 8
    }
    if cfg.MaxInSlot < 2 {
        cfg.MaxInSlot = 128
    }
    lyncs.Config(&lyncs.ConfigType{
        NumWorkers: cfg.NumSlot,
        MaxInSlot: cfg.MaxInSlot,
        Callbacks: []string{"init", "run"},
        Builtin: map[string]string{
            "krc20": builtinKrc20,
        },
        Debug: false,
    })
    err := fs.WalkDir(fsGenesis, ".", func(path string, d fs.DirEntry, err error) (error) {
        if err != nil || d.IsDir() {
            return err
        }
        op := strings.TrimSuffix(d.Name(), ".lua")
        if stateContractMap["KRC-20_"+op] != nil {
            return nil
        }
        code, err := fsGenesis.ReadFile(path)
        if err != nil {
            return err
        }
        stateContractMap["KRC-20_"+op] = &storage.StateContractType{
            Ca: "KRC-20",
            Op: op,
            Code: code,
            Bc: nil,
            OpMod: 0,
        }
        return nil
    })
    if err != nil {
        log.Fatalln("operation.InitLyncs fatal:", err.Error())
    }
    err = ApplyContractMap(stateContractMap)
    if err != nil {
        log.Fatalln("operation.InitLyncs fatal:", err.Error())
    }
    slog.Info("lyncs ready.")
}

////////////////////////////////
func ApplyContractMap(stateContractMap map[string]*storage.StateContractType) (error) {
    var err error
    for key, stContract := range stateContractMap{
        if len(stContract.Code) <= 0 {
            continue
        }
        slog.Info("Load contract: " + key)
        if stContract.Bc != nil {
            err = lyncs.PoolFromBC(key, stContract.Bc)
        } else {
            err = lyncs.PoolFromCode(key, string(stContract.Code))
        }
        if err != nil {
            return err
        }
    }
    return nil
}

////////////////////////////////
func PrepareStateBatch(stateMap storage.DataStateMapType) (int64, error) {
    mtss := time.Now().UnixMilli()
    _, err := storage.GetStateBatch(stateMap)
    if err != nil {
        return 0, err
    }
    return time.Now().UnixMilli() - mtss, nil
}

////////////////////////////////
func ExecuteBatch(opDataList []storage.DataOperationType, stateMap storage.DataStateMapType, checkpointLast string, testnet bool) (storage.DataRollbackType, int64, error) {
    mtss := time.Now().UnixMilli()
    rollback := storage.DataRollbackType{
        CheckpointBefore: checkpointLast,
        OpScoreList: []uint64{},
        TxIdList: []string{},
    }
    lenOp := len(opDataList)
    if len(opDataList) <= 0 {
        return rollback, 0, nil
    }
    rollback.StateMapBefore = storage.CopyDataStateMap(stateMap)
    callRunList := make([]lyncs.DataCallFuncType, 0, lenOp*12/10)
    for i := range opDataList {
        for j := range opDataList[i].OpScript {
            session := &lyncs.DataSessionType{
                Block: opDataList[i].Block,
                Tx: opDataList[i].Tx,
                TxInputs: opDataList[i].TxInputs,
                TxOutputs: opDataList[i].TxOutputs,
                Op: opDataList[i].Op,
                OpParams: opDataList[i].OpScript[j],
            }
            session.Op["index"] = strconv.Itoa(j)
            callRunList = append(callRunList, lyncs.DataCallFuncType{
                Name: opDataList[i].OpScript[j]["p"] + "_" + opDataList[i].OpScript[j]["op"],
                Fn: "run",
                Session: session,
                KeyRules: opDataList[i].OpKeyRules[j],
            })
        }
    }
    resultMap := make(map[string]map[int]*lyncs.DataResultType, len(callRunList))
    stLineBeforeMap := make(map[string]map[int][]string, len(callRunList))
    stLineAfterMap := make(map[string]map[int][]string, len(callRunList))
    mutex := &sync.RWMutex{}
    lyncs.CallFuncParallel(callRunList, stateMap, nil, nil,
        func(c *lyncs.DataCallFuncType, i int, r *lyncs.DataResultType, err error) (*lyncs.DataResultType) {
            if err != nil {
                r = &lyncs.DataResultType{
                    Op: map[string]string{
                      "score": c.Session.Op["score"],
                      "accept": "-1",
                      "error": err.Error(),
                    },
                }
            }
            stLineBefore := make([]string, 0, len(r.State))
            stLineAfter := make([]string, 0, len(r.State))
            for _, s := range r.State {
                if s == nil {
                    continue
                }
                stLineBefore, stLineAfter = makeStLine(stLineBefore, stLineAfter, c.Session.State[s["_key"]], s)
            }
            index, _ := strconv.Atoi(c.Session.Op["index"])
            mutex.Lock()
            stLineBeforeMap[c.Session.Op["score"]] = map[int][]string{
                index: stLineBefore,
            }
            stLineAfterMap[c.Session.Op["score"]] = map[int][]string{
                index: stLineAfter,
            }
            resultMap[c.Session.Op["score"]] = map[int]*lyncs.DataResultType{
                index: r,
            }
            mutex.Unlock()
            return r
        },
    )
    misc.GoBatch(len(opDataList), func(i int) (error) {
        opData := &opDataList[i]
        iScriptAccept := -1
        opError := ""
        for iScript := range opData.OpScript{
            result := resultMap[opData.Op["score"]][iScript]
            if result.Op["accept"] == "1" && iScriptAccept < 0 {
                iScriptAccept = iScript
            }
            if result.Op["accept"] == "-1" && opError == "" {
                opError = result.Op["error"]
            }
        }
        if iScriptAccept >= 0 {
            opData.Op["accept"] = "1"
            opData.Op["error"] = ""
            if iScriptAccept > 0 {
                opData.OpScript = opData.OpScript[iScriptAccept:]
            }
        } else {
            opData.Op["accept"] = "-1"
            opData.Op["error"] = opError
        }
        if opData.Op["accept"] == "1" {
            var stMapBefore map[string]*string
            var stMapAfter map[string]*string
            opData.StBefore, stMapBefore = mergeStLine(stLineBeforeMap[opData.Op["score"]], false)
            opData.StAfter, stMapAfter = mergeStLine(stLineAfterMap[opData.Op["score"]], true)
            opData.SsInfo = countStLine(stMapBefore, stMapAfter)
        }
        return nil
    })
    
/*for k,v := range resultMap {
fmt.Println("resultMap["+k+"]: ", v[0].Op, v[0].OpParams, v[0].KeyRules, v[0].State)
}
for k,v := range stLineBeforeMap {
fmt.Println("stLineBeforeMap["+k+"]: ", v)
}
for k,v := range stLineAfterMap {
fmt.Println("stLineAfterMap["+k+"]: ", v)
}
for k,v := range opDataList {
fmt.Println("StBefore/StAfter: ", k, v.StBefore, v.StAfter)
fmt.Println("SsInfo: ", v.SsInfo)
}
for k,v := range stateMap {
fmt.Println("stateMap["+k+"]: ", v)
}*/

    for i := range opDataList {
        opData := &opDataList[i]
        if opData.Op["accept"] == "1" {
            cpHeader := opData.Op["score"] +","+ opData.Tx["id"] +","+ opData.Block["hash"] +","+ opData.OpScript[0]["p"] +","+ opData.OpScript[0]["op"]
            sum := blake2b.Sum256([]byte(cpHeader))
            cpHeader = fmt.Sprintf("%064x", string(sum[:]))
            cpState := strings.Join(opData.StAfter, ";")
            sum = blake2b.Sum256([]byte(cpState))
            cpState = fmt.Sprintf("%064x", string(sum[:]))
            sum = blake2b.Sum256([]byte(checkpointLast + cpHeader + cpState))
            opData.Checkpoint = fmt.Sprintf("%064x", string(sum[:]))
            checkpointLast = opData.Checkpoint
            
            // ststats_#KRC-20 -  op|total / tokentotal / feetotal ..
            // ststats_{token} - holdertotal / op|total / top100 ..
            
            // optotal ++
            // optotal_{op} ++
            // optotal_{token} ++
            // optotal_{token}_{op} ++
            // feetotal ++
            
            // IF op:deploy
            //   tokentotal ++
            
            // IF SsInfo.TickAffcMap
            //   holdertotal ++/--
            // IF SsInfo.AddressAffcMap
            //   top100 **
            
            // prepare key list ..
            
        }
        rollback.OpScoreLast, _ = strconv.ParseUint(opData.Op["score"], 10, 64)
        rollback.OpScoreList = append(rollback.OpScoreList, rollback.OpScoreLast)
        rollback.TxIdList = append(rollback.TxIdList, opData.Tx["id"])
    }
    
    // PrepareStateBatch - ststats key lsit ..
    // before - rollback.StateMapBefore ..
    // after - stateMap ..
    
    rollback.CheckpointAfter = checkpointLast
    
fmt.Println("rollback: ", rollback.DaaScoreStart, rollback.DaaScoreEnd, rollback.CheckpointBefore, rollback.CheckpointAfter, rollback.OpScoreLast)

    return rollback, time.Now().UnixMilli() - mtss, nil
}

////////////////////////////////
func countStLine(stMapBefore map[string]*string, stMapAfter map[string]*string) (*storage.DataStatsType) {
    ssInfo := &storage.DataStatsType{
        TickAffc: make([]string, 0, 2),
        AddressAffc: make([]string, 0, 4),
        
        // TickAffcMap / AddressAffc ..
        
    }
    TickAffcMap := make(map[string]int, 2)
    balanceBig := new(big.Int)
    lockedBig := new(big.Int)
    for k, v := range stMapAfter {
        line := strings.Split(k, "_")
        if line[0] == storage.KeyPrefixStateBalance {
            nilBefore := false
            nilAfter := false
            if stMapBefore[k] == nil || *stMapBefore[k] == "" {
                nilBefore = true
            }
            if v == nil || *v == "" {
                nilAfter = true
            }
            if nilBefore && !nilAfter {
                TickAffcMap[line[2]] = TickAffcMap[line[2]] + 1
            } else if !nilBefore && nilAfter {
                TickAffcMap[line[2]] = TickAffcMap[line[2]] - 1
            } else {
                TickAffcMap[line[2]] = TickAffcMap[line[2]]
            }
            total := "=0"
            if !nilAfter {
                stBalance := strings.Split(*v, ",")
                balanceBig.SetString(stBalance[1], 10)
                lockedBig.SetString(stBalance[2], 10)
                balanceBig = balanceBig.Add(balanceBig, lockedBig)
                total = "=" + balanceBig.Text(10)
            }
            ssInfo.AddressAffc = append(ssInfo.AddressAffc, line[1]+"_"+line[2]+total)
            
            // AddressAffcMap ..
            
        }
    }
    for k, v := range TickAffcMap {
        ssInfo.TickAffc = append(ssInfo.TickAffc, k+"="+strconv.Itoa(v))
            
        // TickAffcMap ..
            
    }
    return ssInfo
}

////////////////////////////////
func mergeStLine(stLineList map[int][]string, isAfter bool) ([]string, map[string]*string) {
    stLine := make([]string, 0, 8)
    stMap := make(map[string]*string, 8)
    indexLine := map[string]int{}
    lenSt := len(stLineList)
    for i := 0; i < lenSt; i++ {
        for j := range stLineList[i] {
            line := strings.SplitN(stLineList[i][j], ",", 2)
            v := ""
            if len(line) > 1 {
                v = line[1]
            }
            index, exists := indexLine[line[0]]
            if !exists {
                indexLine[line[0]] = len(stLine)
                stLine = append(stLine, stLineList[i][j])
                stMap[line[0]] = &v
            } else if isAfter {
                stLine[index] = stLineList[i][j]
                stMap[line[0]] = &v
            }
        }
    }
    return stLine, stMap
}

////////////////////////////////
func makeStLine(stLineBefore []string, stLineAfter []string, stBefore map[string]string, stAfter map[string]string) ([]string, []string) {
    key := stAfter["_key"]
    stType := strings.SplitN(key, "_", 2)[0]
    var before string
    var after string
    if stType == storage.KeyPrefixStateToken {
        before = makeStLineToken(key, stBefore, stBefore==nil)
        after = makeStLineToken(key, stAfter, stBefore==nil)
    } else if stType == storage.KeyPrefixStateBalance {
        before = makeStLineBalance(key, stBefore)
        after = makeStLineBalance(key, stAfter)
    } else if stType == storage.KeyPrefixStateMarket {
        before = makeStLineMarket(key, stBefore)
        after = makeStLineMarket(key, stAfter)
    } else if stType == storage.KeyPrefixStateBlacklist {
        before = makeStLineBlacklist(key, stBefore)
        after = makeStLineBlacklist(key, stAfter)
    } else if stType == storage.KeyPrefixStateContract {
        before = makeStLineContract(key, stBefore)
        after = makeStLineContract(key, stAfter)
    //} else if stType == storage.KeyPrefixStateXxx {
        // ...
    }
    stLineBefore = append(stLineBefore, before)
    stLineAfter = append(stLineAfter, after)
    return stLineBefore, stLineAfter
}

////////////////////////////////
func makeStLineToken(key string, stToken map[string]string, isDeploy bool) (string) {
    if stToken == nil || stToken["_key"] != "" && len(stToken) == 1 {
        return key
    }
    list := make([]string, 0, 16)
    list = append(list, key)
    if isDeploy {
        list = append(list, stToken["max"])
        list = append(list, stToken["lim"])
        list = append(list, stToken["pre"])
        list = append(list, stToken["dec"])
        list = append(list, stToken["from"])
        list = append(list, stToken["to"])
    }
    list = append(list, stToken["minted"])
    list = append(list, stToken["opmod"])
    if stToken["mod"] == "issue" {
        list = append(list, stToken["mod"])
        list = append(list, stToken["burned"])
        list = append(list, stToken["name"])
    }
    return strings.Join(list, ",")
}

/*func AppendStLineToken(stLine []string, key string, stToken *storage.StateTokenType, isDeploy bool, isAfter bool) ([]string) {
    keyFull := storage.KeyPrefixStateToken + key
    iExists := -1
    list := []string{}
    for i, line := range stLine {
        list = strings.SplitN(line, ",", 2)
        if list[0] == keyFull {
            iExists = i
            break
        }
    }
    if iExists < 0 {
        return append(stLine, MakeStLineToken(key, stToken, isDeploy))
    }
    if isAfter {
        stLine[iExists] = MakeStLineToken(key, stToken, isDeploy)
    }
    return stLine
}*/

////////////////////////////////
func makeStLineBalance(key string, stBalance map[string]string) (string) {
    if stBalance == nil || stBalance["_key"] != "" && len(stBalance) == 1 {
        return key
    }
    list := make([]string, 0, 8)
    list = append(list, key)
    list = append(list, stBalance["dec"])
    list = append(list, stBalance["balance"])
    list = append(list, stBalance["locked"])
    list = append(list, stBalance["opmod"])
    return strings.Join(list, ",")
}

/*func AppendStLineBalance(stLine []string, key string, stBalance *storage.StateBalanceType, isAfter bool) ([]string) {
    keyFull := storage.KeyPrefixStateBalance + key
    iExists := -1
    list := []string{}
    for i, line := range stLine {
        list = strings.SplitN(line, ",", 2)
        if list[0] == keyFull {
            iExists = i
            break
        }
    }
    if iExists < 0 {
        return append(stLine, MakeStLineBalance(key, stBalance))
    }
    if isAfter {
        stLine[iExists] = MakeStLineBalance(key, stBalance)
    }
    return stLine
}*/

////////////////////////////////
func makeStLineMarket(key string, stMarket map[string]string) (string) {
    if stMarket == nil || stMarket["_key"] != "" && len(stMarket) == 1 {
        return key
    }
    list := make([]string, 0, 8)
    list = append(list, key)
    list = append(list, stMarket["uaddr"])
    list = append(list, stMarket["uamt"])
    list = append(list, stMarket["tamt"])
    list = append(list, stMarket["opadd"])
    return strings.Join(list, ",")
}

/*func AppendStLineMarket(stLine []string, key string, stMarket *storage.StateMarketType, isAfter bool) ([]string) {
    keyFull := storage.KeyPrefixStateMarket + key
    iExists := -1
    list := []string{}
    for i, line := range stLine {
        list = strings.SplitN(line, ",", 2)
        if list[0] == keyFull {
            iExists = i
            break
        }
    }
    if iExists < 0 {
        return append(stLine, MakeStLineMarket(key, stMarket))
    }
    if isAfter {
        stLine[iExists] = MakeStLineMarket(key, stMarket)
    }
    return stLine
}*/

////////////////////////////////
func makeStLineBlacklist(key string, stBlacklist map[string]string) (string) {
    if stBlacklist == nil || stBlacklist["_key"] != "" && len(stBlacklist) == 1 {
        return key
    }
    list := make([]string, 0, 4)
    list = append(list, key)
    list = append(list, stBlacklist["opadd"])
    return strings.Join(list, ",")
}

////////////////////////////////
func makeStLineContract(key string, stContract map[string]string) (string) {
    if stContract == nil || stContract["_key"] != "" && len(stContract) == 1 {
        return key
    }
    list := make([]string, 0, 4)
    list = append(list, key)
    
    // ...
    
    return strings.Join(list, ",")
}

/*func AppendStLineBlacklist(stLine []string, key string, stBlacklist *storage.StateBlacklistType, isAfter bool) ([]string) {
    keyFull := storage.KeyPrefixStateBlacklist + key
    iExists := -1
    list := []string{}
    for i, line := range stLine {
        list = strings.SplitN(line, ",", 2)
        if list[0] == keyFull {
            iExists = i
            break
        }
    }
    if iExists < 0 {
        return append(stLine, MakeStLineBlacklist(key, stBlacklist))
    }
    if isAfter {
        stLine[iExists] = MakeStLineBlacklist(key, stBlacklist)
    }
    return stLine
}

////////////////////////////////
func AppendSsInfoTickAffc(tickAffc []string, key string, value int64) ([]string) {
    iExists := -1
    valueBefore := int64(0)
    list := []string{}
    for i, affc := range tickAffc {
        list = strings.SplitN(affc, "=", 2)
        if list[0] == key {
            iExists = i
            if len(list) > 1 {
                valueBefore, _ = strconv.ParseInt(list[1], 10, 64)
            }
            break
        }
    }
    if iExists < 0 {
        return append(tickAffc, key+"="+strconv.FormatInt(value, 10))
    }
    tickAffc[iExists] = key+"="+strconv.FormatInt(value+valueBefore, 10)
    return tickAffc
}

////////////////////////////////
func AppendSsInfoAddressAffc(addressAffc []string, key string, value string) ([]string) {
    iExists := -1
    list := []string{}
    for i, affc := range addressAffc {
        list = strings.SplitN(affc, "=", 2)
        if list[0] == key {
            iExists = i
            break
        }
    }
    if iExists < 0 {
        return append(addressAffc, key+"="+value)
    }
    addressAffc[iExists] = key+"="+value
    return addressAffc
}*/

////////////////////////////////
/*func ValidateTick(tick *string) (bool) {
    *tick = strings.ToUpper(*tick)
    lenTick := len(*tick)
    if (lenTick < 4 || lenTick > 6) {
        return false
    }
    for i := 0; i < lenTick; i++ {
        if ((*tick)[i] < 65 || (*tick)[i] > 90) {
            return false
        }
    }
    return true
}
////////////////////////////////
func ValidateTxId(tick *string) (bool) {
    *tick = strings.ToLower(*tick)
    if len(*tick) != 64 {
        return false
    }
    _, err := hex.DecodeString(*tick)
    if err != nil {
        return false
    }
    return true
}
////////////////////////////////
func ValidateTickTxId(tick *string) (bool) {
    if len(*tick) < 64 {
        return ValidateTick(tick)
    }
    return ValidateTxId(tick)
}
////////////////////////////////
func ValidateAmount(amount *string) (bool) {
    if *amount == "" {
        *amount = "0"
        return false
    }
    amountBig := new(big.Int)
    _, s := amountBig.SetString(*amount, 10)
    if !s {
        return false
    }
    amount2 := amountBig.Text(10)
    if *amount != amount2 {
        return false
    }
    limitBig := new(big.Int)
    limitBig.SetString("0", 10)
    if limitBig.Cmp(amountBig) >= 0 {
        return false
    }
    limitBig.SetString("99999999999999999999999999999999", 10)
    if amountBig.Cmp(limitBig) > 0 {
        return false
    }
    return true
}

////////////////////////////////
func ValidateDec(dec *string, def string) (bool) {
    if *dec == "" {
        *dec = def
        return true
    }
    decInt, err := strconv.Atoi(*dec)
    if err != nil {
        return false
    }
    decString := strconv.Itoa(decInt)
    if (decString != *dec || decInt < 0 || decInt > 18) {
        return false
    }
    return true
}

////////////////////////////////
func ValidationUint(value *string, def string) (bool) {
    if *value == "" {
        *value = def
        return true
    }
    valueUint, err := strconv.ParseUint(*value, 10, 64)
    if err != nil {
        return false
    }
    valueString := strconv.FormatUint(valueUint, 10)
    if (valueString != *value) {
        return false
    }
    return true
}*/

// ...
