
////////////////////////////////
package storage

//#include "rocksdb/c.h"
import "C"
import (
    "sync"
    "time"
    "kasplex-executor/config"
)

////////////////////////////////
const KeyPrefixStateToken = "sttoken"
const KeyPrefixStateBalance = "stbalance"
const KeyPrefixStateMarket = "stmarket"
const KeyPrefixStateBlacklist = "stblacklist"
const KeyPrefixStateContract = "stcontract"
const KeyPrefixStateStats = "ststats"
// KeyPrefixStateXxx ...

var KeyPrefixStateMap = map[string]bool{
    KeyPrefixStateToken: true,
    KeyPrefixStateBalance: true,
    KeyPrefixStateMarket: true,
    KeyPrefixStateBlacklist: true,
    KeyPrefixStateContract: true,
    KeyPrefixStateStats: true,
    // KeyPrefixStateXxx: true,
}

////////////////////////////////
func GetStateBatch(stateMap DataStateMapType) (int64, error) {
    lenState := len(stateMap)
    keyList := make([]string, 0, lenState)
    for key := range stateMap {
        keyList = append(keyList, key)
    }
    mutex := new(sync.RWMutex)
    mtsBatch, err := doGetBatchCF(nil, cfState, keyList, func(i int, val []byte) (error) {
        if val == nil {
            return nil
        }
        decoded, err := ConvStateToStringMap(keyList[i], val)
        if err != nil {
            return err
        }
        mutex.Lock()
        stateMap[keyList[i]] = decoded
        mutex.Unlock()
        return nil
    })
    if err != nil {
        return 0, err
    }
    return mtsBatch, nil
}

////////////////////////////////
func SaveStateBatchRocks(tx *C.rocksdb_transaction_t, stRowMap map[string]*DataKvRowType) (error) {
    var err error
    for _, row := range stRowMap {
        if row == nil {
            continue
        }
        if len(row.Val) == 0 {
            err = deleteCF(tx, cfState, row.Key)
            if err != nil {
                return err
            }
            continue
        }
        err = putCF(tx, cfState, row.Key, row.Val, 0)
        if err != nil {
            return err
        }
    }
    return nil
}

////////////////////////////////
func SaveExecutionBatch(opDataList []DataOperationType, stRowMap map[string]*DataKvRowType, vspcList []DataVspcType, rollback *DataRollbackType, synced bool) ([]int64, error) {
    mtsBatchList := [4]int64{}
    mtsBatchList[0] = time.Now().UnixMilli()
    tx := txBegin()
    err := SaveStateBatchRocks(tx, stRowMap)
    if err != nil {
        txRollback(tx)
        return nil, err
    }
    mtsBatchList[1] = time.Now().UnixMilli()
    var iddKeyList []string
    iddKeyList, err = SaveIndexBatchRocks(tx, opDataList)
    if err != nil {
        txRollback(tx)
        return nil, err
    }
    mtsBatchList[2] = time.Now().UnixMilli()
    err = SetRuntimeVspcLast(tx, vspcList)
    if err != nil {
        txRollback(tx)
        return nil, err
    }
    rollback.IddKeyList = iddKeyList
    err = SetRuntimeRollbackLast(tx, rollback)
    if err != nil {
        txRollback(tx)
        return nil, err
    }
    lenVspc := len(vspcList)
    stateSynced := &DataSyncedType{
        Synced: synced,
        OpScore: rollback.OpScoreLast,
        TxId: rollback.TxIdLast,
        Checkpoint: rollback.CheckpointAfter,
        DaaScore: vspcList[lenVspc-1].DaaScore,
        Version: config.Version,
    }
    err = SetRuntimeSynced(tx, stateSynced)
    if err != nil {
        txRollback(tx)
        return nil, err
    }
    mtsBatchList[3] = time.Now().UnixMilli()
    err = txCommit(tx, true)
    if err != nil {
        return nil, err
    }
    SetDaaScoreLastRocks(stateSynced.DaaScore)
    mtsBatchList[0] = mtsBatchList[1] - mtsBatchList[0]
    mtsBatchList[1] = mtsBatchList[2] - mtsBatchList[1]
    mtsBatchList[2] = mtsBatchList[3] - mtsBatchList[2]
    mtsBatchList[3] = time.Now().UnixMilli() - mtsBatchList[3]
    return mtsBatchList[:], nil
}

////////////////////////////////
func RollbackExecutionBatch(daaScore uint64) (int64, error) {
    mtss := time.Now().UnixMilli()
    stRowMap := make(map[string]*DataKvRowType, 128)
    deleteList := make([][]byte, 0, 128)
    // get rollback/vspc first to check and avoid delete all ..
    daaScoreStart := daaScore
    var keyEnd []byte
    for {
        done := false
        keyList, rollbackList, err := GetRuntimeRollbackLast(7, keyEnd)
        if err != nil {
            return 0, err
        }
        lenRollback := len(rollbackList)
        if lenRollback == 0 {
            break
        }
        keyEnd = keyList[0]
        for i := lenRollback-1; i >= 0; i-- {
            if rollbackList[i].DaaScoreEnd < daaScore {
                done = true
                break
            }
            daaScoreStart = rollbackList[i].DaaScoreStart
            deleteList = append(deleteList, keyList[i])
            for j := range rollbackList[i].IddKeyList {
                deleteList = append(deleteList, []byte(rollbackList[i].IddKeyList[j]))
            }
            for key, row := range rollbackList[i].StRowMapBefore {
                stRowMap[key] = row
            }
        }
        if done {
            break
        }
    }
    deleteListVspc, err := GetRuntimeVspcKeyList(daaScoreStart, 0, false)
    deleteList = append(deleteList, deleteListVspc...)
    tx := txBegin()
    err = SaveStateBatchRocks(tx, stRowMap)
    if err != nil {
        txRollback(tx)
        return 0, err
    }
    for _, key := range deleteList {
        err = deleteCF(tx, cfIndex, key)
        if err != nil {
            txRollback(tx)
            return 0, err
        }
    }
    err = txCommit(tx, true)
    if err != nil {
        return 0, err
    }
    SetDaaScoreLastRocks(daaScoreStart)
    return time.Now().UnixMilli() - mtss, nil
}
