
////////////////////////////////
package operation

import (
    "fmt"
    "sync"
    "time"
    "sort"
    "embed"
    "io/fs"
    "strconv"
    "strings"
    "log/slog"
    "math/big"
    "encoding/json"
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
const lenHolderTopMax = 200

////////////////////////////////
func InitLyncs(cfg config.LyncsConfig, stateContractMap map[string]*storage.StateContractType) (error) {
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
        return err
    }
    err = ApplyContractMap(stateContractMap)
    if err != nil {
        return err
    }
    slog.Info("lyncs ready.")
    return nil
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
func ExecuteBatch(opDataList []storage.DataOperationType, stateMap storage.DataStateMapType, checkpointLast string, stCommitmentLast string, testnet bool) (storage.DataRollbackType, map[string]*storage.DataKvRowType, int64, error) {
    mtss := time.Now().UnixMilli()
    rollback := storage.DataRollbackType{
        CheckpointBefore: checkpointLast,
        StCommitmentBefore: stCommitmentLast,
    }
    lenOp := len(opDataList)
    if len(opDataList) <= 0 {
        return rollback, nil, 0, nil
    }

fmt.Println("mts = ", time.Now().UnixMilli())
    
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
    stRowBeforeMap := make(map[string]map[int][]*storage.DataKvRowType, len(callRunList))
    stRowAfterMap := make(map[string]map[int][]*storage.DataKvRowType, len(callRunList))
    mutex := &sync.RWMutex{}

fmt.Println("mts = ", time.Now().UnixMilli())
    
    lyncs.CallFuncParallel(callRunList, stateMap, nil, nil,
        func(c *lyncs.DataCallFuncType, i int, r *lyncs.DataResultType, err error) (*lyncs.DataResultType) {
            if err != nil {
                
fmt.Println("error: ", err.Error())
                
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
            stRowBefore := make([]*storage.DataKvRowType, 0, len(r.State))
            stRowAfter := make([]*storage.DataKvRowType, 0, len(r.State))
            for _, s := range r.State {
                if s == nil {
                    continue
                }
                stLineBefore, stLineAfter = makeStLine(stLineBefore, stLineAfter, c.Session.State[s["_key"]], s)
                stRowBefore, stRowAfter = makeStRow(stRowBefore, stRowAfter, c.Session.State[s["_key"]], s)
            }
            index, _ := strconv.Atoi(c.Session.Op["index"])
            mutex.Lock()
            stLineBeforeMap[c.Session.Op["score"]] = map[int][]string{
                index: stLineBefore,
            }
            stLineAfterMap[c.Session.Op["score"]] = map[int][]string{
                index: stLineAfter,
            }
            stRowBeforeMap[c.Session.Op["score"]] = map[int][]*storage.DataKvRowType{
                index: stRowBefore,
            }
            stRowAfterMap[c.Session.Op["score"]] = map[int][]*storage.DataKvRowType{
                index: stRowAfter,
            }
            resultMap[c.Session.Op["score"]] = map[int]*lyncs.DataResultType{
                index: r,
            }
            mutex.Unlock()
            return r
        },
    )

fmt.Println("mts = ", time.Now().UnixMilli())
    
    misc.GoBatch(len(opDataList), func(i int, iBatch int) (error) {
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
            var stMapBefore map[string]string
            var stMapAfter map[string]string
            opData.StBefore, opData.StRowBefore, stMapBefore = mergeStLineMap(stLineBeforeMap[opData.Op["score"]], stRowBeforeMap[opData.Op["score"]], false)
            opData.StAfter, opData.StRowAfter, stMapAfter = mergeStLineMap(stLineAfterMap[opData.Op["score"]], stRowAfterMap[opData.Op["score"]], true)
            opData.SsInfo = countStLine(stMapBefore, stMapAfter)
        }
        return nil
    })

fmt.Println("mts = ", time.Now().UnixMilli())
        
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

    stRowMapBefore := make(map[string]*storage.DataKvRowType, lenOp*4)
    stRowMapAfter := make(map[string]*storage.DataKvRowType, lenOp*4)
    stStatsMap := make(map[string]*storage.StateStatsType, 16)
    stCommitmentLastByte := []byte(stCommitmentLast)
    mhQueue := misc.MuHashNew(1024)
    for i := range opDataList {
        opData := &opDataList[i]
        if opData.Op["accept"] == "1" {
            cpHeader := opData.Op["score"] +","+ opData.Tx["id"] +","+ opData.Block["hash"] +","+ opData.OpScript[0]["p"] +","+ opData.OpScript[0]["op"]
            sum := blake2b.Sum256([]byte(cpHeader))
            cpHeader = fmt.Sprintf("%064x", string(sum[:]))
            
            cpState := strings.Join(opData.StAfter, ";")
            sum = blake2b.Sum256([]byte(cpState))
            cpState = fmt.Sprintf("%064x", string(sum[:]))
            
            mhQueue = mhQueue[:0]
            for _, row := range opData.StRowBefore {
                mhQueue = misc.MuHashRemove(mhQueue, *row.P)
            }
            for _, row := range opData.StRowAfter {
                mhQueue = misc.MuHashAdd(mhQueue, *row.P)
            }
            stCommitmentLastByte, _ = misc.MuHashSerialize(stCommitmentLastByte, mhQueue, false)
            opData.StCommitment = string(stCommitmentLastByte)
            
            // replace to StCommitment in Checkpoint In the future HF ...
            
            sum = blake2b.Sum256([]byte(checkpointLast + cpHeader + cpState))
            opData.Checkpoint = fmt.Sprintf("%064x", string(sum[:]))
            
            checkpointLast = opData.Checkpoint
            calculateStStats(opData, stateMap, stStatsMap, stRowMapBefore)
            stRowMapBefore = appendStRowList(stRowMapBefore, opData.StRowBefore, false)
            stRowMapAfter = appendStRowList(stRowMapAfter, opData.StRowAfter, true)
        }
        rollback.OpScoreLast, _ = strconv.ParseUint(opData.Op["score"], 10, 64)
        rollback.TxIdLast = opData.Tx["id"]
    }
    updateStStats(stStatsMap, stRowMapAfter)
    rollback.StRowMapBefore = stRowMapBefore
    rollback.CheckpointAfter = checkpointLast
    rollback.StCommitmentAfter = string(stCommitmentLastByte)

fmt.Println("mts = ", time.Now().UnixMilli())
        
/*for k,v := range stateMap {
fmt.Println("stateMap["+k+"]: ", v)
}
fmt.Println("rollback: ", rollback.DaaScoreStart, rollback.DaaScoreEnd, rollback.CheckpointBefore, rollback.CheckpointAfter, rollback.OpScoreLast)*/

    return rollback, stRowMapAfter, time.Now().UnixMilli() - mtss, nil
}

////////////////////////////////
func countStLine(stMapBefore map[string]string, stMapAfter map[string]string) (*storage.DataStatsType) {
    ssInfo := &storage.DataStatsType{
        TickAffc: make([]string, 0, 2),
        AddressAffc: make([]string, 0, 4),
        TickAffcMap: make(map[string]int, 2),
        AddressAffcMap: make(map[string]map[string]string, 2),
    }
    TickAffcMap := make(map[string]int, 2)
    balanceBig := new(big.Int)
    lockedBig := new(big.Int)
    for k, v := range stMapAfter {
        line := strings.Split(k, "_")
        if line[0] == storage.KeyPrefixStateBalance {
            nilBefore := false
            nilAfter := false
            if stMapBefore[k] == "" {
                nilBefore = true
            }
            if v == "" {
                nilAfter = true
            }
            if nilBefore && !nilAfter {
                TickAffcMap[line[2]] = TickAffcMap[line[2]] + 1
            } else if !nilBefore && nilAfter {
                TickAffcMap[line[2]] = TickAffcMap[line[2]] - 1
            } else {
                TickAffcMap[line[2]] = TickAffcMap[line[2]]
            }
            total := "0"
            if !nilAfter {
                stBalance := strings.Split(v, ",")
                balanceBig.SetString(stBalance[1], 10)
                lockedBig.SetString(stBalance[2], 10)
                balanceBig = balanceBig.Add(balanceBig, lockedBig)
                total = balanceBig.Text(10)
            }
            ssInfo.AddressAffc = append(ssInfo.AddressAffc, line[1]+"_"+line[2]+"="+total)
            if ssInfo.AddressAffcMap[line[2]] == nil {
                ssInfo.AddressAffcMap[line[2]] = make(map[string]string, 4)
            }
            ssInfo.AddressAffcMap[line[2]][line[1]] = total
        }
    }
    for k, v := range TickAffcMap {
        ssInfo.TickAffcMap[k] = v
        ssInfo.TickAffc = append(ssInfo.TickAffc, k+"="+strconv.Itoa(v))
    }
    return ssInfo
}

////////////////////////////////
func appendStRowList(stRowMap map[string]*storage.DataKvRowType, stRowlist []*storage.DataKvRowType, isAfter bool) (map[string]*storage.DataKvRowType) {
    for _, row := range stRowlist {
        if row == nil || len(row.Key) == 0 {
            continue
        }
        key := string(row.Key)
        _, exists := stRowMap[key]
        if exists && !isAfter {
            continue
        }
        stRowMap[key] = row
    }
    return stRowMap
}

////////////////////////////////
func mergeStLineMap(stLineMap map[int][]string, stRowMap map[int][]*storage.DataKvRowType, isAfter bool) ([]string, []*storage.DataKvRowType, map[string]string) {
    lenSt := len(stLineMap)
    iList := make([]int, 0, lenSt)
    for i := range stLineMap{
        iList = append(iList, i)
    }
    sort.Ints(iList)
    stLine := make([]string, 0, 8)
    stRowList := make([]*storage.DataKvRowType, 0, 8)
    stMap := make(map[string]string, 8)
    indexLine := map[string]int{}
    for i := 0; i < lenSt; i++ {
        for j := range stLineMap[iList[i]] {
            line := strings.SplitN(stLineMap[iList[i]][j], ",", 2)
            v := ""
            if len(line) > 1 {
                v = line[1]
            }
            index, exists := indexLine[line[0]]
            if !exists {
                indexLine[line[0]] = len(stLine)
                stLine = append(stLine, stLineMap[iList[i]][j])
                stRowList = append(stRowList, stRowMap[iList[i]][j])
                stMap[line[0]] = v
            } else if isAfter {
                stLine[index] = stLineMap[iList[i]][j]
                stRowList[index] = stRowMap[iList[i]][j]
                stMap[line[0]] = v
            }
        }
    }
    return stLine, stRowList, stMap
}

////////////////////////////////
func makeStRow(stRowBefore []*storage.DataKvRowType, stRowAfter []*storage.DataKvRowType, stBefore map[string]string, stAfter map[string]string) ([]*storage.DataKvRowType, []*storage.DataKvRowType) {
    key := stAfter["_key"]
    before := storage.ConvStateToKvRow(key, stBefore)
    after := storage.ConvStateToKvRow(key, stAfter)
    if before != nil {
        stRowBefore = append(stRowBefore, before)
    }
    if after != nil {
        stRowAfter = append(stRowAfter, after)
    }
    return stRowBefore, stRowAfter
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


////////////////////////////////
func updateStatsHolderTop(holderTop [][2]string, addrAmtMap map[string]string) ([][2]string) {
    amtBig := new(big.Int)
    topBig := new(big.Int)
    for addr, amt := range addrAmtMap {
        lenHolder := len(holderTop)
        index := -1
        for i := lenHolder-1; i >= 0; i-- {
            if addr == holderTop[i][0] {
                index = i
                break
            }
        }
        if index >= 0 {
            lenHolder --
            for i := index; i < lenHolder; i++ {
                holderTop[i] = holderTop[i+1]
            }
            holderTop = holderTop[:lenHolder]
        }
        index = -1
        amtBig.SetString(amt, 10)
        for i := lenHolder-1; i >= 0; i-- {
            topBig.SetString(holderTop[i][1], 10)
            if amtBig.Cmp(topBig) > 0 {
                index = i
                continue
            }
            break
        }
        if index == -1 && lenHolder < lenHolderTopMax {
            holderTop = append(holderTop, [2]string{addr,amt})
        } else if index >= 0 {
            holderTop = append(holderTop, holderTop[lenHolder-1])
            for i := lenHolder-1; i > index; i-- {
                holderTop[i] = holderTop[i-1]
            }
            holderTop[index] = [2]string{addr,amt}
            if len(holderTop) > lenHolderTopMax {
                holderTop = holderTop[:lenHolderTopMax]
            }
        }
    }
    return holderTop
}

////////////////////////////////
func calculateStStats(opData *storage.DataOperationType, stateMap storage.DataStateMapType, stStatsMap map[string]*storage.StateStatsType, stRowMapBefore map[string]*storage.DataKvRowType) {
    keyKRC20 := storage.KeyPrefixStateStats + "_#KRC-20"
    keys := make([]string, 0, 4)
    keys = append(keys, keyKRC20)
    for _, s := range opData.OpScript {
        if s["tick"] != "" {
            keys = append(keys, storage.KeyPrefixStateStats +"_"+s["tick"])
        }
    }
    for k := range opData.SsInfo.TickAffcMap {
        keys = append(keys, storage.KeyPrefixStateStats +"_"+k)
    }
    for k := range opData.SsInfo.AddressAffcMap {
        keys = append(keys, storage.KeyPrefixStateStats +"_"+k)
    }
    for _, k := range keys {
        if stStatsMap[k] == nil {
            stStatsMap[k] = &storage.StateStatsType{}
            if stateMap[k] != nil && stateMap[k]["data"] != "" {
                dataByte := []byte(stateMap[k]["data"])
                json.Unmarshal(dataByte, stStatsMap[k])
                stRowMapBefore[k] = storage.BuildDataKvRow([]byte(k), dataByte)
            } else {
                stStatsMap[k].OpTotal = make([]storage.StateStatsOpCountType, 0, 16)
                stStatsMap[k].HolderTop = make([][2]string, 0, lenHolderTopMax+1)
                stRowMapBefore[k] = storage.BuildDataKvRow([]byte(k), nil)
            }
            stStatsMap[k].OpTotalMap = make(map[string]uint64, 16)
        }
    }
    fee, _ := strconv.ParseUint(opData.Tx["fee"], 10, 64)
    opMod, _ := strconv.ParseUint(opData.Op["score"], 10, 64)
    stStatsMap[keyKRC20].FeeTotal += fee
    for _, s := range opData.OpScript {
        stStatsMap[keyKRC20].OpTotalMap["all"] += 1
        stStatsMap[keyKRC20].OpTotalMap[s["op"]] += 1
        if s["op"] == "deploy" {
            stStatsMap[keyKRC20].TokenTotal += 1
        }
        stStatsMap[keyKRC20].OpMod = opMod
        if s["tick"] == "" {
            continue
        }
        key := storage.KeyPrefixStateStats + "_" + s["tick"]
        stStatsMap[key].OpTotalMap["all"] += 1
        stStatsMap[key].OpTotalMap[s["op"]] += 1
        stStatsMap[key].FeeTotal += fee
        stStatsMap[key].OpMod = opMod
    }
    for k, v := range opData.SsInfo.TickAffcMap {
        stStatsMap[keyKRC20].HolderTotal = uint64(int64(stStatsMap[keyKRC20].HolderTotal)+int64(v))
        key := storage.KeyPrefixStateStats + "_" + k
        stStatsMap[key].HolderTotal = uint64(int64(stStatsMap[key].HolderTotal)+int64(v))
        stStatsMap[key].OpMod = opMod
    }
    for k, v := range opData.SsInfo.AddressAffcMap {
        key := storage.KeyPrefixStateStats + "_" + k
        stStatsMap[key].HolderTop = updateStatsHolderTop(stStatsMap[key].HolderTop, v)
        stStatsMap[key].OpMod = opMod
    }
}

////////////////////////////////
func updateStStats(stStatsMap map[string]*storage.StateStatsType, stRowMapAfter map[string]*storage.DataKvRowType) {
    for key, stats := range stStatsMap {
        opList := make([]string, 0, len(stats.OpTotalMap))
        for op := range stats.OpTotalMap {
            opList = append(opList, op)
        }
        sort.Strings(opList)
        for _, op := range opList {
            stats.OpTotal = append(stats.OpTotal, storage.StateStatsOpCountType{
                Op: op,
                Count: stats.OpTotalMap[op],
            })
        }
        statsJson, _ := json.Marshal(stats)
        stRowMapAfter[key] = storage.BuildDataKvRow([]byte(key), statsJson)
    }
}
