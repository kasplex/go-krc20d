
////////////////////////////////
package explorer

import (
    "log"
    "time"
    "strconv"
    //"runtime"
    "log/slog"
    "krc20d/storage"
    "krc20d/sequencer"
    "krc20d/operation"
)

////////////////////////////////
var countWrongOP = 0
var countLoopSynced = int64(1)
var countReorg = int64(0)
var loopScan = 300

////////////////////////////////
func scan() {
    
    mtss := time.Now().UnixMilli()
    
    // Use the configured daaScoreRange/blockGenesis if empty vspc list.
    var vspcList []storage.DataVspcType
    lenVspcRuntime := len(eRuntime.vspcList)
    if lenVspcRuntime > 0 {
        vspcList = eRuntime.vspcList
    } else {
        vspcList = append(vspcList, storage.DataVspcType{
            DaaScore: eRuntime.cfg.DaaScoreRange[0][0],
            Hash: eRuntime.cfg.BlockGenesis,
        })
        lenVspcRuntime = 1
    }
    vspcLast := vspcList[lenVspcRuntime-1]
    
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
    
    // Get the vspc/tx data list, use configured sequencer mode.
    synced, _, daaScoreRollback, vspcListNext, txDataList, err := sequencer.GetVspcTxDataList(vspcList)
    if err != nil {
        time.Sleep(300*time.Millisecond)
        return
    }
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
        countReorg ++
        slog.Info("storage.RollbackExecutionBatch", "rollback", strconv.FormatUint(daaScoreRollback,10), "mSecond", strconv.Itoa(int(mtsRollback)))
        return
    }
    lenVspcNext := len(vspcListNext)

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
    /*if daaScoreAvailable - vspcListNext[lenVspcNext-1].DaaScore > lenReorgDaaScoreMax {
        rollback.StRowMapBefore = nil
        rollback.IddKeyList = nil
    }*/
    eRuntime.synced = synced
    
    // Fixed GC trigger.
    //runtime.GC()
    
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
    
    // Fixed GC trigger.
    //runtime.GC()
    
    // Additional delay if state synced.
    mtsLoop := time.Now().UnixMilli() - mtss
    slog.Info("explorer.scan", "lenRuntimeVspc", len(eRuntime.vspcList), "lenRuntimeRollback", len(eRuntime.rollbackList), "lenOperation", len(opDataList), "mSecondLoop", mtsLoop, "rateReorg", strconv.FormatInt(countReorg*1000/countLoopSynced,10)+"pt", "synced", eRuntime.synced)
    if (eRuntime.synced) {
        countLoopSynced ++
        mtsLoop = int64(eRuntime.cfg.LoopDelay) - mtsLoop
        if mtsLoop <=0 {
            return
        }
        time.Sleep(time.Duration(mtsLoop)*time.Millisecond)
    }
}
