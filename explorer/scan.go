
////////////////////////////////
package explorer

import (

    "fmt"
    "io"
    "strings"
    "net/http"
    "syscall"
    "os"
    
    "log"
    "time"
    "strconv"
    "runtime"
    "log/slog"
    "kasplex-executor/storage"
    "kasplex-executor/operation"
)

////////////////////////////////
var lenVspcListMaxAdj = lenVspcListMax
var lenVspcBatch = uint64(lenVspcListMax - lenVspcCheck)
var loopScan = 300

////////////////////////////////
func scan() {
    
    mtss := time.Now().UnixMilli()
    
    // Get the next vspc data list.
    vspcLast := storage.DataVspcType{
        DaaScore: eRuntime.cfg.DaaScoreRange[0][0],
    }
    daaScoreStart := vspcLast.DaaScore
    // Use the last vspc if not empty list.
    lenVspcRuntime := len(eRuntime.vspcList)
    if lenVspcRuntime > 0 {
        vspcLast = eRuntime.vspcList[lenVspcRuntime-1]
        daaScoreStart = vspcLast.DaaScore - lenVspcCheck
        if daaScoreStart < eRuntime.vspcList[0].DaaScore {
            daaScoreStart = eRuntime.vspcList[0].DaaScore
        }
    }
    passed, daaScoreStartNext := checkDaaScoreRange(daaScoreStart)
    if !passed {
        daaScoreStart = daaScoreStartNext - uint64(lenVspcCheck)
    }
    
    // Some things to clean up.
    loopScan ++
    if loopScan > 300 {
        loopScan = 0
        daaScoreLast := vspcLast.DaaScore
        mtsBatchDel, err := storage.DelRuntimeExpired(daaScoreLast)
        if err != nil {
            slog.Warn("storage.DelRuntimeExpired failed.", "error", err.Error())
        } else {
            slog.Debug("storage.DelRuntimeExpired", "mSecond", mtsBatchDel)
        }
    }
    
    // Get the maximum available daascore from cluster db.
    _, _, daaScoreAvailable, err := storage.GetRuntimeChainBlockLast()
    if err != nil {
        slog.Warn("storage.GetRuntimeChainBlockLast failed, sleep 3s.", "error", err.Error())
        time.Sleep(3000*time.Millisecond)
        return
    }
    if daaScoreAvailable <= daaScoreStart + uint64(eRuntime.cfg.Hysteresis + lenVspcCheck + 5) {
        slog.Info("storage.GetRuntimeChainBlockLast empty.", "daaScoreAvailable", daaScoreAvailable)
        time.Sleep(1550*time.Millisecond)
        return
    }
    // Calculate the maximum available vspc length.
    lenVspcListMaxAvailable := int(daaScoreAvailable - daaScoreStart - uint64(eRuntime.cfg.Hysteresis) - 5)
    if lenVspcListMaxAdj > lenVspcListMaxAvailable {
        lenVspcListMaxAdj = lenVspcListMaxAvailable
    }
    // Get next vspc data list from cluster db.
    vspcListNext, mtsBatchVspc, err := storage.GetNodeVspcList(daaScoreStart, lenVspcListMaxAdj+5)
    if err != nil {
        slog.Warn("storage.GetNodeVspcList failed, sleep 3s.", "daaScore", daaScoreStart, "error", err.Error())
        time.Sleep(3000*time.Millisecond)
        return
    }
    // Ignore the last reserved vspc data if synced, reduce the probability of vspc-reorg.
    lenVspcNext := len(vspcListNext)
    if lenVspcNext <= 0 {
        slog.Debug("storage.GetNodeVspcList empty.", "daaScore", daaScoreStart)
        time.Sleep(1550*time.Millisecond)
        return
    }
    slog.Info("storage.GetNodeVspcList", "daaScoreAvailable", daaScoreAvailable, "daaScoreStart", daaScoreStart, "lenBlock/mSecond", strconv.Itoa(lenVspcNext)+"/"+strconv.Itoa(int(mtsBatchVspc)), "lenVspcListMax", lenVspcListMaxAdj, "synced", eRuntime.synced)

    // Check vspc list if need rollback.
    daaScoreRollback, vspcListNext := checkRollback(eRuntime.vspcList, vspcListNext, daaScoreStart)
    if daaScoreRollback > 0 {
        storage.ProcessISD(daaScoreRollback)
        mtsRollback := int64(0)
        mtsRollback, err = storage.RollbackExecutionBatch(daaScoreRollback)
        if err != nil {
            slog.Warn("storage.RollbackExecutionBatch failed, sleep 3s.", "error", err.Error())
            time.Sleep(3000*time.Millisecond)
            return
        }
        err = initRuntime(false)
        if err != nil {
            log.Fatalln("explorer.initRuntime fatal: ", err)
            return
        }
        slog.Info("storage.RollbackExecutionBatch", "start/rollback", strconv.FormatUint(daaScoreStart,10)+"/"+strconv.FormatUint(daaScoreRollback,10), "mSecond", strconv.Itoa(int(mtsRollback)))
        return
    } else if vspcListNext == nil {
        lenVspcListMaxAdj += 50
        if lenVspcListMaxAdj > lenVspcListRuntimeMax {
            lenVspcListMaxAdj = lenVspcListRuntimeMax
        }
        slog.Debug("storage.checkDaaScoreRollback empty.", "daaScore", daaScoreStart, "lenVspcListMax", lenVspcListMaxAdj)
        eRuntime.synced = false
        time.Sleep(1750*time.Millisecond)
        return
    }
    lenVspcListMaxAdj = lenVspcListMax
    lenVspcNext = len(vspcListNext)
    slog.Debug("explorer.checkRollback", "start/next", strconv.FormatUint(daaScoreStart,10)+"/"+strconv.FormatUint(vspcListNext[0].DaaScore,10))
    
    // Extract and get the transaction list.
    daaScoreNextBatch := uint64(0)
    vspcRemoveIndex := 0
    txDataList := make([]storage.DataTransactionType, 0, 512)
    for i, vspc := range vspcListNext {
        if vspc.DaaScore <= vspcLast.DaaScore {
            continue
        }
        passed, _ := checkDaaScoreRange(vspc.DaaScore)
        if !passed {
            continue
        }
        if daaScoreNextBatch == 0 {
            daaScoreNextBatch = (vspc.DaaScore/lenVspcBatch+1) * lenVspcBatch
        } else if vspc.DaaScore >= daaScoreNextBatch {
            vspcRemoveIndex = i
            break
        }
        for _, txId := range vspc.TxIdList {
            txDataList = append(txDataList, storage.DataTransactionType{
                TxId: txId,
                DaaScore: vspc.DaaScore,
                BlockAccept: vspc.Hash,
                BlockTime: vspc.Timestamp,
            })
        }
    }
    if vspcRemoveIndex > 0 {
        vspcListNext = vspcListNext[:vspcRemoveIndex]
    }
    lenVspcNext = len(vspcListNext)
    
    // Get the transaction data list from cluster db.
    lenTxData := len(txDataList)
    txDataList, mtsBatchTx, err := storage.GetNodeTransactionDataList(txDataList)
    if err != nil {
        slog.Warn("storage.GetNodeTransactionDataList failed, sleep 3s.", "lenTransaction", lenTxData, "error", err.Error())
        time.Sleep(3000*time.Millisecond)
        return
    }
    slog.Info("storage.GetNodeTransactionDataList", "lenTransaction/mSecond", strconv.Itoa(lenTxData)+"/"+strconv.Itoa(int(mtsBatchTx)))
    
    // Parse the transaction and prepare the state key for OP.
    opDataList, stateMap, mtsBatchOp, err := ParseOpDataList(txDataList)
    if err != nil {
        slog.Warn("explorer.ParseOpDataList failed, sleep 3s.", "error", err.Error())
        time.Sleep(3000*time.Millisecond)
        return
    }
    slog.Info("explorer.ParseOpDataList", "lenOperation/lenState/mSecond", strconv.Itoa(len(opDataList))+"/"+strconv.Itoa(len(stateMap))+"/"+strconv.Itoa(int(mtsBatchOp)))

    // Prepare the op data list.
    mtsBatchSt, err := operation.PrepareStateBatch(stateMap)
    if err != nil {
        slog.Warn("operation.PrepareStateBatch failed, sleep 3s.", "error", err.Error())
        time.Sleep(3000*time.Millisecond)
        return
    }
    slog.Debug("operation.PrepareStateBatch", "lenState/mSecond", strconv.Itoa(len(stateMap))+"/"+strconv.Itoa(int(mtsBatchSt)))
    // Execute the op list and generate the rollback data.
    checkpointLast := ""
    stCommitmentLast := ""
    lenRollbackList := len(eRuntime.rollbackList)
    if lenRollbackList > 0 {
        checkpointLast = eRuntime.rollbackList[lenRollbackList-1].CheckpointAfter
        stCommitmentLast = eRuntime.rollbackList[lenRollbackList-1].StCommitmentAfter
    }
    rollback, stRowMap, mtsBatchExe, err := operation.ExecuteBatch(opDataList, stateMap, checkpointLast, stCommitmentLast, eRuntime.testnet)
    if err != nil {
        slog.Warn("operation.ExecuteBatch failed, sleep 3s.", "error", err.Error())
        time.Sleep(3000*time.Millisecond)
        return
    }
    rollback.DaaScoreStart = vspcListNext[0].DaaScore
    rollback.DaaScoreEnd = vspcListNext[lenVspcNext-1].DaaScore
    if rollback.OpScoreLast == 0 {
        rollback.OpScoreLast = eRuntime.opScoreLast
        rollback.TxIdLast = eRuntime.txIdLast
    } else {
        eRuntime.opScoreLast = rollback.OpScoreLast
        eRuntime.txIdLast = rollback.TxIdLast
    }
    slog.Debug("operation.ExecuteBatch", "checkpoint", rollback.CheckpointAfter, "lenOperation/mSecond", strconv.Itoa(len(opDataList))+"/"+strconv.Itoa(int(mtsBatchExe)))
    eRuntime.synced = false
    /*if daaScoreAvailable - vspcListNext[lenVspcNext-1].DaaScore > lenReorgDaaScoreMax {
        rollback.StRowMapBefore = nil
        rollback.IddKeyList = nil
    }*/
    if daaScoreAvailable - vspcListNext[lenVspcNext-1].DaaScore < uint64(lenVspcListMax+eRuntime.cfg.Hysteresis) {
        eRuntime.synced = true
    }
    
////////////////////////////
opScoreCheck := strconv.FormatUint(rollback.OpScoreLast,10)
for i := len(opDataList)-1; i >= 0; i-- {
    if opDataList[i].Op["accept"] == "1" {
        opScoreCheck = opDataList[i].Op["score"]
        break
    }
}
fmt.Println("Execution Batch - ", "daaScore: ", rollback.DaaScoreStart, rollback.DaaScoreEnd, "checkpoint: ", rollback.CheckpointBefore, rollback.CheckpointAfter, "stCommitment: ", rollback.StCommitmentBefore, rollback.StCommitmentAfter, "opScoreLast: ", rollback.OpScoreLast, "size: ", len(rollback.StRowMapBefore), len(rollback.IddKeyList))
fmt.Println("")
url := "https://api-24353568745345.kasplex.org/v1/krc20/op/"+opScoreCheck
fmt.Println("Checking: ", url)
r, e := http.Get(url)
wrong := false
if e == nil {
    defer r.Body.Close()
    if r.StatusCode == http.StatusOK {
        rr, e := io.ReadAll(r.Body)
        if e == nil {
            rrs := string(rr)
            if strings.Contains(rrs, rollback.CheckpointAfter) {
                fmt.Println("#### CheckpointAfter Identical ####")
            } else {
                fmt.Println("%%%% CheckpointAfter Wrong %%%%")
                wrong = true
            }
        } else {
            fmt.Printf("io.ReadAll failed: ", e.Error())
        }
    } else {
        fmt.Printf("http.Get failed: ", r.StatusCode)
    }
} else {
    fmt.Printf("http.Get error: ", e.Error())
}
fmt.Println("")
//var input string
//fmt.Println("Press any Key to Return ..")
//fmt.Scanln(&input)
if wrong {
    end := len(opDataList)
    start := 0
    mid := end / 2
    for {
        if (end-start) < 100 {
            break
        }
        i := mid
        if opDataList[i].Op["accept"] != "1" {
            mid ++
            continue
        }
        url := "https://api-24353568745345.kasplex.org/v1/krc20/op/"+opDataList[i].Tx["id"]
        r, e := http.Get(url)
        if e != nil {
            fmt.Println("error: ", e)
            continue
        }
        rr, e := io.ReadAll(r.Body)
        if e != nil {
            fmt.Println("error: ", e)
            r.Body.Close()
            continue
        }
        rrs := string(rr)
        if !strings.Contains(rrs, opDataList[i].Checkpoint) {
            fmt.Print("%")
            r.Body.Close()
            end = mid + 1
            mid = (end-start) / 2 + start
            continue
        }
        fmt.Print("#")
        r.Body.Close()
        start = mid
        mid = (end-start) / 2 + start
        continue
    }
    indexWrong := -1
    for i := start; i < end; i++ {
        url := "https://api-24353568745345.kasplex.org/v1/krc20/op/"+opDataList[i].Tx["id"]
        fmt.Print(".")
        r, e := http.Get(url)
        if e != nil {
            fmt.Println("error: ", e)
            continue
        }
        rr, e := io.ReadAll(r.Body)
        if e != nil {
            fmt.Println("error: ", e)
            r.Body.Close()
            continue
        }
        rrs := string(rr)
        if opDataList[i].Op["accept"] == "-1" && strings.Contains(rrs, `"opAccept":"-1"`) {
            r.Body.Close()
            continue
        }
        if !strings.Contains(rrs, opDataList[i].Checkpoint) {
            indexWrong = i
            r.Body.Close()
            break
        }
        r.Body.Close()
    }
    if indexWrong >= 0 {
        start = indexWrong - 5
        end = indexWrong + 5
        if start < 0 {
            start = 0
        }
        if end >= len(opDataList) {
            end = len(opDataList) - 1
        }
        for i := start; i <= end; i++ {
            fmt.Println("Wrong OP: ", opDataList[i])
        }
        //fmt.Println("Press any Key to Return ..")
        //fmt.Scanln(&input)
        syscall.Kill(os.Getpid(), syscall.SIGTERM)
        time.Sleep(345*time.Millisecond)
        return
    }
}
////////////////////////////
    
    // Fixed GC trigger.
    runtime.GC()
    
    // Save the execution result data.
    mtsBatchList, err := storage.SaveExecutionBatch(opDataList, stRowMap, vspcListNext, &rollback, eRuntime.synced)
    if err != nil {
        eRuntime.synced = false
        slog.Warn("storage.SaveExecutionBatch failed, sleep 3s.", "error", err.Error())
        time.Sleep(3000*time.Millisecond)
        return
    }
    slog.Debug("operation.SaveExecutionBatch", "mSecondList", strconv.Itoa(int(mtsBatchList[0]))+"/"+strconv.Itoa(int(mtsBatchList[1]))+"/"+strconv.Itoa(int(mtsBatchList[2]))+"/"+strconv.Itoa(int(mtsBatchList[3])))
    
    // Update the runtime data.
    eRuntime.vspcList = append(eRuntime.vspcList, vspcListNext...)
    lenStart := len(eRuntime.vspcList) - lenVspcListRuntimeMax
    if lenStart > 0 {
        eRuntime.vspcList = eRuntime.vspcList[lenStart:]
    }
    eRuntime.rollbackList = append(eRuntime.rollbackList, rollback)
    lenRollback := len(eRuntime.rollbackList)
    if lenRollback > 1 {
        eRuntime.rollbackList = eRuntime.rollbackList[lenRollback-1:]
    }
    
    // Update the ISD status.
    storage.ProcessISD(0)
    
    // Additional delay if state synced.
    mtsLoop := time.Now().UnixMilli() - mtss
    slog.Info("explorer.scan", "lenRuntimeVspc", len(eRuntime.vspcList), "lenRuntimeRollback", len(eRuntime.rollbackList), "lenOperation", len(opDataList), "mSecondLoop", mtsLoop, "synced", eRuntime.synced)
    if (eRuntime.synced) {
        mtsLoop = 850 - mtsLoop
        if mtsLoop <=0 {
            return
        }
        time.Sleep(time.Duration(mtsLoop)*time.Millisecond)
    }
}

////////////////////////////////
func checkRollback(vspcListPrev []storage.DataVspcType, vspcListNext []storage.DataVspcType, daaScoreStart uint64) (uint64, []storage.DataVspcType) {
    if len(vspcListPrev) <= 0 {
        return 0, vspcListNext
    }
    var vspcList1 []storage.DataVspcType
    var vspcList2 []storage.DataVspcType
    for i := range vspcListPrev {
        if vspcListPrev[i].DaaScore < daaScoreStart {
            continue
        }
        vspcList1 = vspcListPrev[i:]
        break
    }
    lenCheck := len(vspcList1)
    if lenCheck > 0 {
        if len(vspcListNext) <= lenCheck {
            return 0, nil
        } else {
            vspcList2 = vspcListNext[:lenCheck]
        }
    } else {
        return 0, vspcListNext
    }
    for i := 0; i < lenCheck; i ++ {
        if (vspcList1[i].DaaScore != vspcList2[i].DaaScore || vspcList1[i].Hash != vspcList2[i].Hash) {
            return vspcList1[i].DaaScore, vspcListPrev[:(len(vspcListPrev)-lenCheck+i)]
        }
    }
    return 0, vspcListNext[lenCheck:]
}

////////////////////////////////
func checkDaaScoreRange(daaScore uint64) (bool, uint64) {
    for _, dRange := range eRuntime.cfg.DaaScoreRange {
        if daaScore < dRange[0] {
            return false, dRange[0]
        } else if (daaScore <= dRange[1]) {
            return true, daaScore
        }
    }
    return false, daaScore
}
