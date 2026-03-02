
////////////////////////////////
package storage

//#include "rocksdb/c.h"
import "C"
import (
    "fmt"
    "time"
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
    err := seekCF(nil, cfIndex, []byte(keyPrefixRuntimeVspc), []byte(keyPrefixRuntimeVspc+"`"), lenVspc, true, nil, func(i int, key []byte, val []byte) (bool, error) {
        data := DataVspcType{}
        err := json.Unmarshal(val, &data)
        if err != nil {
            return false, err
        }
        keyByte := make([]byte, len(key))
        copy(keyByte, key)
        keyList = append(keyList, keyByte)
        dataList = append(dataList, data)
        return true, nil
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
    err := seekCF(nil, cfIndex, keyStart, keyEnd, maxCount, dsc, nil, func(i int, key []byte, val []byte) (bool, error) {
        keyByte := make([]byte, len(key))
        copy(keyByte, key)
        keyList = append(keyList, keyByte)
        return true, nil
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
        err = putCF(tx, cfIndex, []byte(key), val, 0)
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
    err := seekCF(nil, cfIndex, []byte(keyPrefixRuntimeRollback), keyEnd, lenRollback, true, nil, func(i int, key []byte, val []byte) (bool, error) {
        data := DataRollbackType{}
        err := json.Unmarshal(val, &data)
        if err != nil {
            return false, err
        }
        keyByte := make([]byte, len(key))
        copy(keyByte, key)
        keyList = append(keyList, keyByte)
        dataList = append(dataList, data)
        return true, nil
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
    return putCF(tx, cfIndex, []byte(key), val, 0)
}

////////////////////////////////
// Delete the expired vspc/rollback data list.
func DelRuntimeExpired(daaScoreLast uint64) (int64, error) {
    mts := time.Now().UnixMilli()
    daaScoreLast -= dtlIndexRuntime
    keyStart := keyPrefixRuntimeVspc
    keyEnd := keyPrefixRuntimeVspc + "_" + fmt.Sprintf("%020d",daaScoreLast)
    err := deleteRangeCF(cfIndex, []byte(keyStart), []byte(keyEnd))
    if err != nil {
        return 0, err
    }
    keyStart = keyPrefixRuntimeRollback
    keyEnd = keyPrefixRuntimeRollback + "_" + fmt.Sprintf("%020d",daaScoreLast)
    err = deleteRangeCF(cfIndex, []byte(keyStart), []byte(keyEnd))
    if err != nil {
        return 0, err
    }
    return time.Now().UnixMilli()-mts, nil
}

////////////////////////////////
// Get the sync state.
func GetRuntimeSynced() (*DataSyncedType, error) {
    val, err := getCF(nil, cfIndex, []byte(keyPrefixRuntimeSynced), nil)
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
