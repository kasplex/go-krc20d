
////////////////////////////////
package storage

//#include "rocksdb/c.h"
import "C"
import (
    "fmt"
    "slices"
    "strconv"
)

////////////////////////////////
const dtlIndexRuntime = 864000
const keyPrefixRuntimeVspc = "rtdvspc"
const keyPrefixRuntimeRollback = "rtdrollback"
const keyPrefixRuntimeSynced = "rtdsynced"

////////////////////////////////
// Get the last processed vspc data list.
func GetRuntimeVspcLast(lenVspc int) ([][]byte, []DataVspcType, error) {
    keyList := make([][]byte, 0, lenVspc)
    dataList := make([]DataVspcType, 0, lenVspc)
    err := seekCF(nil, cfIndex, []byte(keyPrefixRuntimeVspc), []byte(keyPrefixRuntimeVspc+"`"), lenVspc, true, func(i int, key []byte, val []byte) (error) {
        data := DataVspcType{}
        err := json.Unmarshal(val, &data)
        if err != nil {
            return err
        }
        keyByte := make([]byte, len(key))
        copy(keyByte, key)
        keyList = append(keyList, keyByte)
        dataList = append(dataList, data)
        return nil
    })
    if err != nil {
        return nil, nil, err
    }
    slices.Reverse(keyList)
    slices.Reverse(dataList)
    return keyList, dataList, nil
}

////////////////////////////////
// Get the vspc key list by daaScore seeking.
func GetRuntimeVspcKeyList(daaScore uint64, maxCount int, dsc bool) ([][]byte, error) {
    keyList := make([][]byte, 0, 32)
    var keyStart []byte
    var keyEnd []byte
    if dsc {
        keyEnd = []byte(keyPrefixRuntimeVspc+ "_" + fmt.Sprintf("%020d",daaScore))
        keyStart = []byte(keyPrefixRuntimeVspc)
    } else {
        keyStart = []byte(keyPrefixRuntimeVspc+ "_" + fmt.Sprintf("%020d",daaScore))
        keyEnd = []byte(keyPrefixRuntimeVspc+"`")
    }
    err := seekCF(nil, cfIndex, keyStart, keyEnd, maxCount, dsc, func(i int, key []byte, val []byte) (error) {
        keyByte := make([]byte, len(key))
        copy(keyByte, key)
        keyList = append(keyList, keyByte)
        return nil
    })
    if err != nil {
        return nil, err
    }
    if dsc {
        slices.Reverse(keyList)
    }
    return keyList, nil
}

////////////////////////////////
// Set the last processed vspc data list.
func SetRuntimeVspcLast(tx *C.rocksdb_transaction_t, list []DataVspcType) (error) {
    for i := range list {
        key := keyPrefixRuntimeVspc + "_" + fmt.Sprintf("%020d",list[i].DaaScore)
        val, err := json.Marshal(&list[i])
        if err != nil {
            return err
        }
        daaScore := list[i].DaaScore
        if dtlIndexRuntime > sRuntime.cfgRocks.DtlIndex {
            daaScore += dtlIndexRuntime - sRuntime.cfgRocks.DtlIndex
        } else {
            daaScore -= sRuntime.cfgRocks.DtlIndex - dtlIndexRuntime
        }
        err = putCF(tx, cfIndex, []byte(key), val, daaScore)
        if err != nil {
            return err
        }
    }
    return nil
}

////////////////////////////////
// Get the last rollback data list.
func GetRuntimeRollbackLast(lenRollback int, keyEnd []byte) ([][]byte, []DataRollbackType, error) {
    if len(keyEnd) == 0 {
        keyEnd = []byte(keyPrefixRuntimeRollback + "`")
    }
    keyList := make([][]byte, 0, lenRollback)
    dataList := make([]DataRollbackType, 0, lenRollback)
    err := seekCF(nil, cfIndex, []byte(keyPrefixRuntimeRollback), keyEnd, lenRollback, true, func(i int, key []byte, val []byte) (error) {
        data := DataRollbackType{}
        err := json.Unmarshal(val, &data)
        if err != nil {
            return err
        }
        keyByte := make([]byte, len(key))
        copy(keyByte, key)
        keyList = append(keyList, keyByte)
        dataList = append(dataList, data)
        return nil
    })
    if err != nil {
        return nil, nil, err
    }
    slices.Reverse(keyList)
    slices.Reverse(dataList)
    return keyList, dataList, nil
}

////////////////////////////////
// Set the last rollback data.
func SetRuntimeRollbackLast(tx *C.rocksdb_transaction_t, rollback *DataRollbackType) (error) {
    key := keyPrefixRuntimeRollback + "_" + fmt.Sprintf("%020d",rollback.DaaScoreStart) + "_" + strconv.FormatUint(rollback.DaaScoreEnd,10)
    val, err := json.Marshal(rollback)
    if err != nil {
        return err
    }
    daaScore := rollback.DaaScoreEnd
    if dtlIndexRuntime > sRuntime.cfgRocks.DtlIndex {
        daaScore += dtlIndexRuntime - sRuntime.cfgRocks.DtlIndex
    } else {
        daaScore -= sRuntime.cfgRocks.DtlIndex - dtlIndexRuntime
    }
    return putCF(tx, cfIndex, []byte(key), val, daaScore)
}

////////////////////////////////
// Get the sync state.
func GetRuntimeSynced() (*DataSyncedType, error) {
    val, err := getCF(nil, cfIndex, []byte(keyPrefixRuntimeSynced))
    if err != nil {
        return nil, err
    }
    data := &DataSyncedType{}
    err = json.Unmarshal(val, data)
    if err != nil {
        return nil, err
    }
    return data, nil
}

////////////////////////////////
// Set the sync state.
func SetRuntimeSynced(tx *C.rocksdb_transaction_t, data *DataSyncedType) (error) {
    val, err := json.Marshal(data)
    if err != nil {
        return err
    }
    return putCF(tx, cfIndex, []byte(keyPrefixRuntimeSynced), val, 0)
}

////////////////////////////////
// Get runtime data from table "runtime", in the cluster db.
func GetRuntimeCassa(key string) (string, string, string, error) {
    row := sRuntime.sessionCassa.Query(cqlnGetRuntime, key)
    defer row.Release()
    var k0, v1, v2, v3 string
    err := row.Scan(&k0, &v1, &v2, &v3)
    if err != nil {
        if err.Error() == "not found"{
            return "", "", "", nil
        }
        return "", "", "", err
    }
    return v1, v2, v3, nil
}

////////////////////////////////
// Get the last updated virtual chain block state.
func GetRuntimeChainBlockLast() (string, uint64, uint64, error) {
    hash, blueScore, daaScore, err := GetRuntimeCassa("H_CBLOCK_LAST")
    if err != nil {
        return "", 0, 0, err
    }
    intBlueScore, _ := strconv.ParseUint(blueScore, 10, 64)
    intDaaScore, _ := strconv.ParseUint(daaScore, 10, 64)
    return hash, intBlueScore, intDaaScore, nil
}

// ...
