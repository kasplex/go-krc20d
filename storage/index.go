
////////////////////////////////
package storage

//#include "rocksdb/c.h"
import "C"
import (
    "fmt"
    "sync"
    "unsafe"
    "slices"
    "strconv"
    "kasplex-executor/misc"
)

////////////////////////////////
const OpRangeBy = uint64(100000)
const pageSizeIndex = 50

////////////////////////////////
const KeyPrefixIndexOpTxid = "iddoptxid"
const KeyPrefixIndexOpScore = "iddopscore"
const KeyPrefixIndexToken = "iddtoken"
const KeyPrefixIndexAddress = "iddaddr"
// KeyPrefixIndexXxx ...

////////////////////////////////
func CompactIndex() {
    CompactCF(cfIndex)
}

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
    if sRuntime.cfgRocks.DtlFailed > 0 && sRuntime.cfgRocks.DtlIndex > sRuntime.cfgRocks.DtlFailed {
        dtlOffset = sRuntime.cfgRocks.DtlIndex - sRuntime.cfgRocks.DtlFailed
    }
    mutex := new(sync.Mutex)
    misc.GoBatch(lenOpData, func(i int, iBatch int) (error) {
        txIdByte := unsafe.Slice(unsafe.StringData(opDataList[i].Tx["id"]), len(opDataList[i].Tx["id"]))
        keyOpTxid := KeyPrefixIndexOpTxid + "_" + opDataList[i].Tx["id"]
        opScore, _ := strconv.ParseUint(opDataList[i].Op["score"], 10, 64)
        opScoreString := fmt.Sprintf("%020d", opScore)
        opRange := opScore / OpRangeBy
        opIndex := opScore - opRange*OpRangeBy
        keyOpScore := KeyPrefixIndexOpScore + "_" + fmt.Sprintf("%015d",opRange) + "_" + fmt.Sprintf("%05d",opIndex)
        rowOpTxid := ConvIndexOpDataToKvRow(keyOpTxid, &opDataList[i])
        rowOpScore := BuildDataKvRow(unsafe.Slice(unsafe.StringData(keyOpScore),len(keyOpScore)), txIdByte)
        keyToken := ""
        rowToken := &DataKvRowType{}
        if opDataList[i].Op["accept"] == "1" && opDataList[i].OpScript[0]["op"] == "deploy" {
            keyToken = KeyPrefixIndexToken + "_" + opScoreString
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
                }
            }
            for j := range keyAddrList {
                rowAddrList = append(rowAddrList, BuildDataKvRow(unsafe.Slice(unsafe.StringData(keyAddrList[j]),len(keyAddrList[j])),txIdByte))
            }
        }
        daaScore := opScore / 10000
        if opDataList[i].Op["accept"] == "-1" && daaScore > dtlOffset {
            daaScore -= dtlOffset
        }
        mutex.Lock()
        keyList = append(keyList, keyOpTxid, keyOpScore)
        rowList = append(rowList, rowOpTxid, rowOpScore)
        daaScoreList = append(daaScoreList, daaScore, daaScore)
        if keyToken != "" {
            keyList = append(keyList, keyToken)
            rowList = append(rowList, rowToken)
            daaScoreList = append(daaScoreList, 0)
        }
        for j := range keyAddrList {
            keyList = append(keyList, keyAddrList[j])
            rowList = append(rowList, rowAddrList[j])
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
func RebuildIndexTokenRocks() (error) {
    keyStart := []byte(KeyPrefixStateToken + "_")
    keyEnd := []byte(KeyPrefixStateToken + "`")
    err := seekCF(nil, cfState, keyStart, keyEnd, 0, false, nil, func(i int, key []byte, val []byte) (bool, error) {
        if len(val) == 0 {
            return true, nil
        }
        decoded, err := ConvStateToStringMap(string(key), val)
        if err != nil {
            return false, err
        }
        if decoded["tick"] == "" || decoded["opadd"] == "" {
            return true, nil
        }
        opAdd, _ := strconv.ParseUint(decoded["opadd"], 10, 64);
        keyToken := KeyPrefixIndexToken + "_" + fmt.Sprintf("%020d",opAdd)
        rowToken := BuildDataKvRow(unsafe.Slice(unsafe.StringData(keyToken),len(keyToken)), unsafe.Slice(unsafe.StringData(decoded["tick"]),len(decoded["tick"])))
        err = putCF(nil, cfIndex, rowToken.Key, rowToken.Val, 0)
        if err != nil {
            return false, err
        }
        return true, nil
    })
    if err != nil {
        return err
    }
    return nil
}

////////////////////////////////
func checkDataExpired(daaScore uint64) (error) {
    if sRuntime.cfgRocks.DtlIndex == 0 {
        return nil
    }
    daaScoreLast := GetDaaScoreLastRocks()
    if daaScoreLast == 0 {
        return fmt.Errorf("unsynced")
    }
    if daaScoreLast > sRuntime.cfgRocks.DtlIndex && daaScore < daaScoreLast-sRuntime.cfgRocks.DtlIndex {
        return fmt.Errorf("data expired")
    }
    return nil
}

////////////////////////////////
func getDaaScoreExpired() (uint64) {
    if sRuntime.cfgRocks.DtlIndex == 0 {
        return 0
    }
    daaScoreLast := GetDaaScoreLastRocks()
    if daaScoreLast == 0 {
        return 0
    }
    if daaScoreLast > sRuntime.cfgRocks.DtlIndex {
        return daaScoreLast-sRuntime.cfgRocks.DtlIndex
    }
    return 0
}

////////////////////////////////
func GetOpDataMap(txIdList []string) (map[string]*DataIndexOperationType, error) {
    lenTxId := len(txIdList)
    opDataMap := make(map[string]*DataIndexOperationType, lenTxId)
    if lenTxId == 0 {
        return opDataMap, nil
    }
    keyList := make([]string, lenTxId)
    for i := range txIdList {
        keyList[i] = KeyPrefixIndexOpTxid + "_" + txIdList[i]
    }
    daaScoreExpired := getDaaScoreExpired()
    mutex := new(sync.RWMutex)
    _, err := doGetBatchCF(nil, cfIndex, keyList, func(i int, val []byte) (error) {
        data := DataIndexOperationType{}
        err := json.Unmarshal(val, &data)
        if err != nil {
            return err
        }
        if data.State.OpScore/10000 > daaScoreExpired {
            data.TxId = txIdList[i]
            mutex.Lock()
            opDataMap[txIdList[i]] = &data
            mutex.Unlock()
        }
        return nil
    })
    if err != nil {
        return nil, err
    }
    return opDataMap, nil
}

////////////////////////////////
func GetOpListByOpRange(opRange string) ([]*DataIndexOperationType, error) {
    intOpRange, _ := strconv.ParseUint(opRange, 10, 64)
    err := checkDataExpired(intOpRange*10)
    if err != nil {
        return nil, err
    }
    keyOpScore := KeyPrefixIndexOpScore + "_" + fmt.Sprintf("%015d",intOpRange)
    txIdList := make([]string, 0, 64)
    err = seekCF(nil, cfIndex, []byte(keyOpScore+"_"), []byte(keyOpScore+"`"), 0, false, nil, func(i int, key []byte, val []byte) (bool, error) {
        txIdList = append(txIdList, string(val))
        return true, nil
    })
    if err != nil {
        return nil, err
    }
    opDataMap, err := GetOpDataMap(txIdList)
    if err != nil {
        return nil, err
    }
    opList := make([]*DataIndexOperationType, 0, len(txIdList))
    for i := range txIdList {
        if opDataMap[txIdList[i]] == nil {
            continue
        }
        opList = append(opList, opDataMap[txIdList[i]])
    }
    return opList, nil
}

////////////////////////////////
func GetOpTxIdByOpScore(opScore uint64) (string, error) {
    err := checkDataExpired(opScore/10000)
    if err != nil {
        return "", err
    }
    opRange := opScore / OpRangeBy
    opIndex := opScore - opRange*OpRangeBy
    txId := ""
    keyOpScore := KeyPrefixIndexOpScore + "_" + fmt.Sprintf("%015d",opRange) + "_" + fmt.Sprintf("%05d",opIndex)
    _, err = getCF(nil, cfIndex, []byte(keyOpScore), func(val []byte) (error) {
        txId = string(val)
        return nil
    })
    if err != nil {
        return "", err
    }
    return txId, nil
}

////////////////////////////////
func GetOpTxIdListByOpIndex(address string, tick string, opScoreNext uint64, goPrev bool) ([]string, error) {
    txIdList := make([]string, 0, pageSizeIndex)
    key := KeyPrefixIndexAddress + "_"
    if address != "" && tick != "" {
        key = key + address + "_" + tick
    } else if address != "" {
        key = key + address
    } else {
        key = key + tick
    }
    var keyStart []byte
    var keyEnd []byte
    dsc := !goPrev
    if dsc {
        keyStart = []byte(key + "_")
        keyEnd = []byte(key + "_" + fmt.Sprintf("%020d", opScoreNext))
    } else {
        opScoreNext ++
        keyStart = []byte(key + "_" + fmt.Sprintf("%020d", opScoreNext))
        keyEnd = []byte(key + "`")
    }
    err := seekCF(nil, cfIndex, keyStart, keyEnd, pageSizeIndex, dsc, nil, func(i int, key []byte, val []byte) (bool, error) {
        txIdList = append(txIdList, string(val))
        return true, nil
    })
    if err != nil {
        return nil, err
    }
    if goPrev {
        slices.Reverse(txIdList)
    }
    return txIdList, nil
}

////////////////////////////////
func GetTickListByOpAdd(opAddNext uint64, goPrev bool) ([]string, error) {
    tickList := make([]string, 0, pageSizeIndex)
    var keyStart []byte
    var keyEnd []byte
    dsc := !goPrev
    if dsc {
        keyStart = []byte(KeyPrefixIndexToken + "_")
        keyEnd = []byte(KeyPrefixIndexToken + "_" + fmt.Sprintf("%020d", opAddNext))
    } else {
        opAddNext ++
        keyStart = []byte(KeyPrefixIndexToken + "_" + fmt.Sprintf("%020d", opAddNext))
        keyEnd = []byte(KeyPrefixIndexToken + "`")
    }
    err := seekCF(nil, cfIndex, keyStart, keyEnd, pageSizeIndex, dsc, nil, func(i int, key []byte, val []byte) (bool, error) {
        tickList = append(tickList, string(val))
        return true, nil
    })
    if err != nil {
        return nil, err
    }
    if goPrev {
        slices.Reverse(tickList)
    }
    return tickList, nil
}

////////////////////////////////
func SeekIndexRaw(key string, maxCount int, dsc bool, keyOnly bool) ([]string, []string, error) {
    if maxCount <= 0 || maxCount > pageSizeIndex {
        maxCount = pageSizeIndex
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
    err := seekCF(nil, cfIndex, keyStart, keyEnd, maxCount, dsc, nil, func(i int, key []byte, val []byte) (bool, error) {
        stKeyList = append(stKeyList, string(key))
        if keyOnly {
            stValList = append(stValList, "")
        } else {
            stValList = append(stValList, string(val))
        }
        return true, nil
    })
    if err != nil {
        return nil, nil, err
    }
    return stKeyList, stValList, nil
}
