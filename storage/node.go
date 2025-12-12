
////////////////////////////////
package storage

import (
    "sync"
    "sort"
    "strconv"
    "strings"
    "encoding/json"
    "github.com/gocql/gocql"
    "kasplex-executor/protowire"
)

////////////////////////////////
// Get the timestamp map of the blocks, use the node archive db.
func GetNodeBlockTimestampMap(timestampMap map[string]uint64) (int64, error) {
    lenBlock := len(timestampMap)
    hashList := make([]string, 0, lenBlock)
    for hash := range timestampMap {
        hashList = append(hashList, hash)
    }
    mutex := new(sync.RWMutex)
    mtsBatch, err := startQueryBatchInCassa(lenBlock, func(iStart int, iEnd int, session *gocql.Session) (error) {
        hashIn := []string{}
        for i := iStart; i < iEnd; i ++ {
            hashIn = append(hashIn, "'"+hashList[i]+"'")
        }
        cql := strings.Replace(cqlnGetBlockHeader, "{hashIn}", strings.Join(hashIn,","), 1)
        row := session.Query(cql).Iter().Scanner()
        for row.Next() {
            var hash string
            var headerJson string
            err := row.Scan(&hash, &headerJson)
            if err != nil {
                return err
            }
            header := protowire.RpcBlockHeader{}
            err = json.Unmarshal([]byte(headerJson), &header)
            if err != nil {
                return err
            }
            mutex.Lock()
            timestampMap[hash] = uint64(header.Timestamp)
            mutex.Unlock()
        }
        return row.Err()
    })
    if err != nil {
        return 0, err
    }
    return mtsBatch, nil
}

////////////////////////////////
// Get the next vspc data list, use the node archive db.
func GetNodeVspcList(daaScoreStart uint64, lenBlock int) ([]DataVspcType, int64, error) {
    vspcMap := make(map[uint64]*DataVspcType, lenBlock)
    timestampMap := make(map[string]uint64, lenBlock)
    mutex := new(sync.RWMutex)
    mtsBatch, err := startQueryBatchInCassa(lenBlock, func(iStart int, iEnd int, session *gocql.Session) (error) {
        daaScoreList := []string{}
        for i := iStart; i < iEnd; i ++ {
            daaScoreList = append(daaScoreList, strconv.FormatUint(daaScoreStart+uint64(i),10))
        }
        daaScoreIn := strings.Join(daaScoreList, ",")
        cql := strings.Replace(cqlnGetVspcData, "{daascoreIn}", daaScoreIn, 1)
        row := session.Query(cql).Iter().Scanner()
        for row.Next() {
            var daaScore uint64
            var hash string
            var txId string
            err := row.Scan(&daaScore, &hash, &txId)
            if err != nil {
                return err
            }
            txIdList := []string{}
            if txId != "" {
                txIdList = strings.Split(txId, ",")
            }
            if daaScore < 110165000 {
                sort.Strings(txIdList)
            }
            mutex.Lock()
            vspcMap[daaScore] = &DataVspcType{
                DaaScore: daaScore,
                Hash: hash,
                TxIdList: txIdList,
            }
            timestampMap[hash] = 0
            mutex.Unlock()
        }
        return row.Err()
    })
    if err != nil {
        return nil, 0, err
    }
    _, err = GetNodeBlockTimestampMap(timestampMap)
    vspcList := make([]DataVspcType, 0, lenBlock)
    for i := daaScoreStart; i < daaScoreStart+uint64(lenBlock); i ++ {
        if vspcMap[i] == nil {
            continue
        }
        vspcMap[i].Timestamp = timestampMap[vspcMap[i].Hash]
        vspcList = append(vspcList, *vspcMap[i])
    }
    return vspcList, mtsBatch, nil
}

////////////////////////////////
// Get the data map of the transaction in vspc, use the node archive db.
func GetNodeTransactionDataMap(txDataList []DataTransactionType) (map[string]*protowire.RpcTransaction, int64, error) {
    txDataMap := map[string]*protowire.RpcTransaction{}
    mutex := new(sync.RWMutex)
    mtsBatch, err := startQueryBatchInCassa(len(txDataList), func(iStart int, iEnd int, session *gocql.Session) (error) {
        txIdList := []string{}
        for i := iStart; i < iEnd; i ++ {
            txIdList = append(txIdList, "'"+txDataList[i].TxId+"'")
        }
        txIdIn := strings.Join(txIdList, ",")
        cql := strings.Replace(cqlnGetTransactionData, "{txidIn}", txIdIn, 1)
        row := session.Query(cql).Iter().Scanner()
        for row.Next() {
            var txId string
            var dataJson string
            err := row.Scan(&txId, &dataJson)
            if err != nil {
                return err
            }
            data := protowire.RpcTransaction{}
            err = json.Unmarshal([]byte(dataJson), &data)
            if err != nil {
                return err
            }
            mutex.Lock()
            txDataMap[txId] = &data
            mutex.Unlock()
        }
        return row.Err()
    })
    if err != nil {
        return nil, 0, err
    }
    return txDataMap, mtsBatch, nil
}

////////////////////////////////
// Get the data list of the transaction in vspc, use the node archive db.
func GetNodeTransactionDataList(txDataList []DataTransactionType) ([]DataTransactionType, int64, error) {
    txDataMap, mtsBatch, err := GetNodeTransactionDataMap(txDataList)
    if err != nil {
        return nil, 0, err
    }
    for i, txData := range txDataList {
        if txDataMap[txData.TxId] == nil {
            continue
        }
        txDataList[i].Data = txDataMap[txData.TxId]
    }
    return txDataList, mtsBatch, nil
}

// ...
