
////////////////////////////////
package storage

//#include "rocksdb/c.h"
import "C"
import (
    "sync"
    "time"
    "slices"
    "kasplex-executor/config"
)

////////////////////////////////
const pageSizeState = 50

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

////////////////////////////////
func GetStateTokenMap(tickList []string) (DataStateMapType, error) {
    lenTick := len(tickList)
    stTokenMap := make(DataStateMapType, lenTick)
    if lenTick == 0 {
        return stTokenMap, nil
    }
    for i := range tickList {
        stTokenMap[KeyPrefixStateToken+"_"+tickList[i]] = nil
    }
    _, err := GetStateBatch(stTokenMap)
    if err != nil {
        return nil, err
    }
    return stTokenMap, nil
}

////////////////////////////////
func GetStateStatsData(tick string) (*StateStatsType, error) {
    keyStats := KeyPrefixStateStats + "_" + tick
    stStatsMap := DataStateMapType{keyStats:nil}
    _, err := GetStateBatch(stStatsMap)
    if err != nil {
        return nil, err
    }
    if stStatsMap[keyStats] == nil || stStatsMap[keyStats]["data"] == "" {
        return nil, nil
    }
    statsData := &StateStatsType{}
    err = json.Unmarshal([]byte(stStatsMap[keyStats]["data"]), statsData)
    if err != nil {
        return nil, err
    }
    return statsData, nil
}

////////////////////////////////
func getStateToStringMap(key string) (map[string]string, error) {
    var stData map[string]string
    _, err := getCF(nil, cfState, []byte(key), func(val []byte) (error) {
        if val == nil {
            return nil
        }
        decoded, err := ConvStateToStringMap(key, val)
        if err != nil {
            return err
        }
        stData = decoded
        return nil
    })
    if err != nil {
        return nil, err
    }
    return stData, nil
}

////////////////////////////////
func seekStateToStringMapList(keyStart []byte, keyEnd []byte, dsc bool, reverse bool) ([]map[string]string, error) {
    stDataList := make([]map[string]string, 0, pageSizeState)
    err := seekCF(nil, cfState, keyStart, keyEnd, pageSizeState, dsc, func(i int, key []byte, val []byte) (bool, error) {
        if val == nil {
            return true, nil
        }
        decoded, err := ConvStateToStringMap(string(key), val)
        if err != nil {
            return false, err
        }
        stDataList = append(stDataList, decoded)
        return true, nil
    })
    if err != nil {
        return nil, err
    }
    if reverse {
        slices.Reverse(stDataList)
    }
    return stDataList, nil
}

////////////////////////////////
func GetStateAddressBalanceList(address string, tickNext string, goPrev bool) ([]map[string]string, error) {
    key := KeyPrefixStateBalance + "_" + address
    var keyStart []byte
    var keyEnd []byte
    if goPrev {
        keyStart = []byte(key + "_" + tickNext + " ")
        keyEnd = []byte(key + "`")
    } else {
        keyStart = []byte(key + "_")
        keyEnd = []byte(key + "_" + tickNext)
    }
    return seekStateToStringMapList(keyStart, keyEnd, !goPrev, goPrev)
}

////////////////////////////////
func GetStateAddressBalanceData(address string, tick string) (map[string]string, error) {
    return getStateToStringMap(KeyPrefixStateBalance + "_" + address + "_" + tick)
}

////////////////////////////////
func GetStateBlacklistList(tick string, addressNext string, goPrev bool) ([]map[string]string, error) {
    key := KeyPrefixStateBlacklist + "_" + tick
    var keyStart []byte
    var keyEnd []byte
    if goPrev {
        keyStart = []byte(key + "_" + addressNext + " ")
        keyEnd = []byte(key + "`")
    } else {
        keyStart = []byte(key + "_")
        keyEnd = []byte(key + "_" + addressNext)
    }
    return seekStateToStringMapList(keyStart, keyEnd, !goPrev, goPrev)
}

////////////////////////////////
func GetStateBlacklistData(tick string, address string) (map[string]string, error) {
    return getStateToStringMap(KeyPrefixStateBlacklist + "_" + tick + "_" + address)
}

////////////////////////////////
func GetStateMarketList(tick string, address string, addressTxIdNext string, goPrev bool) ([]map[string]string, error) {
    key := KeyPrefixStateMarket + "_" + tick
    var keyStart []byte
    var keyEnd []byte
    if goPrev {
        if address == "" {
            keyStart = []byte(key + "_")
        } else {
            keyStart = []byte(key + "_" + address + "_")
        }
        keyEnd = []byte(key + "_" + addressTxIdNext)
    } else {
        if addressTxIdNext == "" && address != "" {
            addressTxIdNext = address + "_"
        }
        keyStart = []byte(key + "_" + addressTxIdNext + " ")
        if address == "" {
            keyEnd = []byte(key + "`")
        } else {
            keyEnd = []byte(key + "_" + address + "`")
        }
    }
    return seekStateToStringMapList(keyStart, keyEnd, goPrev, goPrev)
}

////////////////////////////////
func GetStateMarketData(tick string, address string, txId string) (map[string]string, error) {
    return getStateToStringMap(KeyPrefixStateMarket + "_" + tick + "_" + address + "_" + txId)
}

////////////////////////////////
func SeekStateRaw(key string, maxCount int, dsc bool) ([]string, []string, error) {
    if maxCount <= 0 || maxCount > pageSizeState {
        maxCount = pageSizeState
    }
    var keyStart []byte
    var keyEnd []byte
    if dsc {
        keyEnd = []byte(key + " ")
    } else {
        keyStart = []byte(key)
    }
    stKeyList := make([]string, 0, maxCount)
    stValList := make([]string, 0, maxCount)
    err := seekCF(nil, cfState, keyStart, keyEnd, maxCount, dsc, func(i int, key []byte, val []byte) (bool, error) {
        stKeyList = append(stKeyList, string(key))
        stValList = append(stValList, string(val))
        return true, nil
    })
    if err != nil {
        return nil, nil, err
    }
    return stKeyList, stValList, nil
}
