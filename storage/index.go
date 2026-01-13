
////////////////////////////////
package storage

//#include "rocksdb/c.h"
import "C"
import (
    "fmt"
    "sync"
    "unsafe"
    "strconv"
    "kasplex-executor/misc"
)

////////////////////////////////
const OpRangeBy = uint64(100000)

////////////////////////////////
const KeyPrefixIndexOpTxid = "iddoptxid"
const KeyPrefixIndexOpScore = "iddopscore"
const KeyPrefixIndexToken = "iddtoken"
const KeyPrefixIndexAddress = "iddaddr"
// KeyPrefixIndexXxx ...

////////////////////////////////
func SaveIndexBatchRocks(tx *C.rocksdb_transaction_t, opDataList []DataOperationType) ([]string, error) {
    if sRuntime.cfgRocks.IndexDisabled {
        return nil, nil
    }
    lenOpData := len(opDataList)
    keyList := make([]string, 0, lenOpData*4)
    rowList := make([]*DataKvRowType, 0, lenOpData*4)
    daaScoreList := make([]uint64, 0, lenOpData*4)
    dtlOffset := uint64(0)
    if sRuntime.cfgRocks.DtlIndex > sRuntime.cfgRocks.DtlFailed {
        dtlOffset = sRuntime.cfgRocks.DtlIndex - sRuntime.cfgRocks.DtlFailed
    }
    mutex := new(sync.RWMutex)
    misc.GoBatch(lenOpData, func(i int, iBatch int) (error) {
        txIdByte := unsafe.Slice(unsafe.StringData(opDataList[i].Tx["id"]), len(opDataList[i].Tx["id"]))
        keyOpTxid := KeyPrefixIndexOpTxid + "_" + opDataList[i].Tx["id"]
        opScore, _ := strconv.ParseUint(opDataList[i].Op["score"], 10, 64)
        opScoreString := fmt.Sprintf("%020d", opScore)
        opRange := opScore / OpRangeBy
        opIndex := opScore - opRange*OpRangeBy
        keyOpScore := KeyPrefixIndexOpScore + "_" + fmt.Sprintf("%019d",opRange) + "_" + fmt.Sprintf("%05u",opIndex)
        rowOpTxid := ConvIndexOpDataToKvRow(keyOpTxid, &opDataList[i])
        rowOpScore := BuildDataKvRow(unsafe.Slice(unsafe.StringData(keyOpScore),len(keyOpScore)), txIdByte)
        keyToken := ""
        rowToken := &DataKvRowType{}
        if opDataList[i].Op["accept"] == "1" && opDataList[i].OpScript[0]["op"] == "deploy" {
            keyToken := KeyPrefixIndexToken + "_" + opScoreString
            rowToken = BuildDataKvRow(unsafe.Slice(unsafe.StringData(keyToken),len(keyToken)), unsafe.Slice(unsafe.StringData(opDataList[i].OpScript[0]["tick"]),len(opDataList[i].OpScript[0]["tick"])))
        }
        keyAddrList := make([]string, 0, 6)
        rowAddrList := make([]*DataKvRowType, 0, 6)
        if opDataList[i].SsInfo != nil && len(opDataList[i].SsInfo.AddressAffcMap) > 0 {
            for tick, affc := range opDataList[i].SsInfo.AddressAffcMap {
                if len(affc) == 0 {
                    continue
                }
                for addr := range affc {
                    keyAddrList = append(
                        keyAddrList,
                        KeyPrefixIndexAddress + "_" + tick + "_" + opScoreString,
                        KeyPrefixIndexAddress + "_" + addr + "_" + opScoreString,
                        KeyPrefixIndexAddress + "_" + addr + "_" + tick + "_" + opScoreString,
                    )
                    for j := range keyAddrList {
                        rowAddrList = append(rowAddrList, BuildDataKvRow(unsafe.Slice(unsafe.StringData(keyAddrList[j]),len(keyAddrList[j])),txIdByte))
                    }
                }
            }
        }
        daaScore := opScore / 10000
        if opDataList[i].Op["accept"] == "-1" {
            daaScore -= dtlOffset
        }
        mutex.Lock()
        lenAdded := len(rowList)
        keyList = append(keyList, keyOpTxid, keyOpScore)
        rowList = append(rowList, rowOpTxid, rowOpScore)
        if keyToken != "" {
            keyList = append(keyList, keyToken)
            rowList = append(rowList, rowToken)
        }
        if len(keyAddrList) > 0 {
            keyList = append(keyList, keyAddrList...)
            rowList = append(rowList, rowAddrList...)
        }
        lenAdded = len(rowList) - lenAdded
        for j := 0; j < lenAdded; j++ {
            daaScoreList = append(daaScoreList, daaScore)
        }
        mutex.Unlock()
        return nil
    })
    var err error
    for i := range rowList {
        if rowList[i] == nil {
            continue
        }
        err = putCF(tx, cfIndex, rowList[i].Key, rowList[i].Val, daaScoreList[i])
        if err != nil {
            return nil, err
        }
    }
    return keyList, nil
}

////////////////////////////////
func GetOpListByOpRange(opRange string) ([]*DataIndexOperationType, error) {
    keyOpScore := KeyPrefixIndexOpScore + "_" + fmt.Sprintf("%019s",opRange)
    txIdList := make([]string, 0, 64)
    keyList := make([]string, 0, 64)
    err := seekCF(nil, cfIndex, []byte(keyOpScore+"_"), []byte(keyOpScore+"`"), 0, false, func(i int, key []byte, val []byte) (error) {
        txId := string(val)
        txIdList = append(txIdList, txId)
        keyList = append(keyList, KeyPrefixIndexOpTxid+"_"+txId)
        return nil
    })
    if err != nil {
        return nil, err
    }
    opList := make([]*DataIndexOperationType, len(txIdList))
    mutex := new(sync.RWMutex)
    _, err = doGetBatchCF(nil, cfIndex, keyList, func(i int, val []byte) (error) {
        data := DataIndexOperationType{}
        err := json.Unmarshal(val, &data)
        if err != nil {
            return err
        }
        data.TxId = txIdList[i]
        mutex.Lock()
        opList[i] = &data
        mutex.Unlock()
        return nil
    })
    if err != nil {
        return nil, err
    }
    return opList, nil
}

// ...
