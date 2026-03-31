
////////////////////////////////
package sequencer

import (
    "fmt"
    "time"
    "sync"
    "math"
    "sort"
    "strings"
    "strconv"
    "log/slog"
    "github.com/gocql/gocql"
    "krc20d/config"
    "krc20d/protowire"
    "krc20d/storage"
)

////////////////////////////////
type archiveRuntimeType struct {
    cfg config.CassaConfig
    cassa *gocql.ClusterConfig
    sessionCassa *gocql.Session
}
var archiveRuntime archiveRuntimeType

////////////////////////////////
const archiveInMax = 40
const archiveNumBatchMax = 200
const archiveMtsDelayQuery = 5

////////////////////////////////
const archiveLenVspcListMax = 1000
const archiveLenVspcCheck = 200

////////////////////////////////
var archiveMtsBatchLast = int64(0)

////////////////////////////////
var archiveCqlnGetRuntime = "SELECT * FROM runtime WHERE key=?;"
var archiveCqlnGetVspcData = "SELECT daascore,hash,txid FROM vspc WHERE daascore IN ({daascoreIn});"
var archiveCqlnGetTransactionData = "SELECT txid,data FROM transaction WHERE txid IN ({txidIn});"
var archiveCqlnGetBlockHeader = "SELECT hash,header FROM block WHERE hash IN ({hashIn});"
var archiveCqlnGetVspcByDaaScore = "SELECT hash,txid FROM vspc WHERE daascore=?;"
var archiveCqlnGetBlockByHash = "SELECT header FROM block WHERE hash=?;"
var archiveCqlnGetTransactionByTxid = "SELECT data FROM transaction WHERE txid=?;"

////////////////////////////////
var archiveLenVspcListMaxAdj = archiveLenVspcListMax
var archiveLenVspcBatch = uint64(archiveLenVspcListMax - archiveLenVspcCheck)

////////////////////////////////
func archiveInit(cfg config.CassaConfig) (error) {
    if cfg.Host == "" || cfg.Port <= 0 {
        return fmt.Errorf("config invalid")
    }
    archiveRuntime.cfg = cfg
    hostList := strings.Split(archiveRuntime.cfg.Host, ",")
    archiveRuntime.cassa = gocql.NewCluster(hostList...)
    archiveRuntime.cassa.Port = archiveRuntime.cfg.Port
    archiveRuntime.cassa.Authenticator = gocql.PasswordAuthenticator{
        Username: archiveRuntime.cfg.User,
        Password: archiveRuntime.cfg.Pass,
    }
    if archiveRuntime.cfg.Crt != "" {
        archiveRuntime.cassa.SslOpts = &gocql.SslOptions{
            CaPath: archiveRuntime.cfg.Crt,
            EnableHostVerification: false,
        }
    }
    archiveRuntime.cassa.Timeout = 7 * time.Second
    archiveRuntime.cassa.ConnectTimeout = 13 * time.Second
    archiveRuntime.cassa.SocketKeepalive = 31 * time.Second
    archiveRuntime.cassa.RetryPolicy = &gocql.ExponentialBackoffRetryPolicy{
        NumRetries: 3,
        Min: 300 * time.Millisecond,
        Max: 3 * time.Second,
    }
    archiveRuntime.cassa.Consistency = gocql.LocalQuorum
    archiveRuntime.cassa.DisableInitialHostLookup = false
    archiveRuntime.cassa.NumConns = 5
    archiveRuntime.cassa.Keyspace = archiveRuntime.cfg.Space
    var err error
    archiveRuntime.sessionCassa, err = archiveRuntime.cassa.CreateSession()
    if err != nil {
        return err
    }
    GetSyncStatus = archiveGetRuntimeSynced
    GetVspcTxDataList = archiveGetVspcTxDataList
    GetTxDataMap = archiveGetNodeTransactionDataMap
    GetArchiveVspcTxDataList = archiveGetNodeArchiveVspcTxDataList
    GetArchiveTxData = archiveGetNodeArchiveTxData
    return nil
}

////////////////////////////////
func archiveGetVspcTxDataList(vspcList []storage.DataVspcType) (bool, uint64, uint64, []storage.DataVspcType, []storage.DataTransactionType, error) {
    // Determine the starting daaScore.
    lenVspc := len(vspcList)
    daaScoreStart := vspcList[lenVspc-1].DaaScore
    if lenVspc > 1 {
        daaScoreStart -= archiveLenVspcCheck
        if daaScoreStart < vspcList[0].DaaScore {
            daaScoreStart = vspcList[0].DaaScore
        }
    }
    passed, daaScoreStartNext := checkDaaScoreRange(daaScoreStart)
    if !passed {
        daaScoreStart = daaScoreStartNext - uint64(archiveLenVspcCheck)
    }
    // Get the maximum available daaScore from cluster db.
    _, _, daaScoreAvailable, err := archiveGetRuntimeChainBlockLast()
    if err != nil {
        slog.Warn("sequencer.archiveGetRuntimeChainBlockLast failed, sleep 3s.", "error", err.Error())
        time.Sleep(2700*time.Millisecond)
        return false, 0, 0, nil, nil, err
    }
    if daaScoreAvailable <= daaScoreStart + uint64(hysteresis + archiveLenVspcCheck + 5) {
        slog.Info("sequencer.archiveGetRuntimeChainBlockLast not reached, sleep 0.55s.", "daaScoreAvailable", daaScoreAvailable)
        time.Sleep(250*time.Millisecond)
        return false, daaScoreAvailable, 0, nil, nil, fmt.Errorf("vspc not reached")
    }
    // Calculate the maximum available vspc length.
    lenVspcListMaxAvailable := int(daaScoreAvailable - daaScoreStart - uint64(hysteresis) - 5)
    if archiveLenVspcListMaxAdj > lenVspcListMaxAvailable {
        archiveLenVspcListMaxAdj = lenVspcListMaxAvailable
    }
    // Get next vspc data list from cluster db.
    vspcListNext, mtsBatchVspc, err := archiveGetNodeVspcList(daaScoreStart, archiveLenVspcListMaxAdj+5)
    if err != nil {
        slog.Warn("sequencer.archiveGetNodeVspcList failed, sleep 3s.", "daaScore", daaScoreStart, "error", err.Error())
        time.Sleep(2700*time.Millisecond)
        return false, daaScoreAvailable, 0, nil, nil, err
    }
    lenVspcNext := len(vspcListNext)
    if lenVspcNext == 0 {
        archiveLenVspcListMaxAdj += 100
        if archiveLenVspcListMaxAdj > archiveLenVspcListMax*3 {
            archiveLenVspcListMaxAdj = archiveLenVspcListMax*3
        }
        slog.Debug("sequencer.archiveGetNodeVspcList empty, sleep 0.55s.", "daaScore", daaScoreStart)
        time.Sleep(250*time.Millisecond)
        return false, daaScoreAvailable, 0, nil, nil, fmt.Errorf("nil vspc")
    }
    slog.Info("sequencer.archiveGetNodeVspcList", "daaScoreAvailable", daaScoreAvailable, "daaScoreStart", daaScoreStart, "lenBlock/mSecond", strconv.Itoa(lenVspcNext)+"/"+strconv.Itoa(int(mtsBatchVspc)), "lenVspcListMax", archiveLenVspcListMaxAdj)
    // Check vspc list if a rollback is needed.
    daaScoreRollback, vspcListNext := archiveCheckRollback(vspcList, vspcListNext, daaScoreStart)
    if daaScoreRollback > 0 {
        return true, daaScoreAvailable, daaScoreRollback, nil, nil, nil
    } else if len(vspcListNext) == 0 {
        archiveLenVspcListMaxAdj += 100
        if archiveLenVspcListMaxAdj > archiveLenVspcListMax*3 {
            archiveLenVspcListMaxAdj = archiveLenVspcListMax*3
        }
        slog.Debug("sequencer.archiveCheckRollback empty, sleep 0.55s.", "daaScore", daaScoreStart, "lenVspcListMax", archiveLenVspcListMaxAdj)
        time.Sleep(250*time.Millisecond)
        return false, daaScoreAvailable, 0, nil, nil, fmt.Errorf("nil vspc")
    }
    archiveLenVspcListMaxAdj = archiveLenVspcListMax
    lenVspcNext = len(vspcListNext)
    slog.Debug("sequencer.archiveCheckRollback", "start/next", strconv.FormatUint(daaScoreStart,10)+"/"+strconv.FormatUint(vspcListNext[0].DaaScore,10))
    // Extract and get the transaction list.
    txDataList := make([]storage.DataTransactionType, 0, 256)
    txIdMap := make(map[string]bool, 256)
    for i := range vspcListNext {
        if vspcListNext[i].DaaScore <= vspcList[lenVspc-1].DaaScore {
            continue
        }
        passed, _ := checkDaaScoreRange(vspcListNext[i].DaaScore)
        if !passed {
            continue
        }
        for _, txId := range vspcListNext[i].TxIdList {
            if txIdMap[txId] {
                slog.Warn("sequencer.archiveGetNodeVspcList duplicated, sleep 0.55s.", "daaScore", vspcListNext[i].DaaScore, "txId", txId)
                time.Sleep(250*time.Millisecond)
                return false, daaScoreAvailable, 0, nil, nil, fmt.Errorf("tx duplicated")
            }
            txDataList = append(txDataList, storage.DataTransactionType{
                TxId: txId,
                DaaScore: vspcListNext[i].DaaScore,
                BlockAccept: vspcListNext[i].Hash,
                BlockTime: vspcListNext[i].Timestamp,
            })
            txIdMap[txId] = true
        }
    }
    lenTxData := len(txDataList)
    // Get the transaction data list from cluster db.
    txDataList, mtsBatchTx, err := archiveGetNodeTransactionDataList(txDataList)
    if err != nil {
        slog.Warn("sequencer.archiveGetNodeTransactionDataList failed, sleep 3s.", "lenTransaction", lenTxData, "error", err.Error())
        time.Sleep(2700*time.Millisecond)
        return false, daaScoreAvailable, 0, nil, nil, err
    }
    slog.Info("sequencer.archiveGetNodeTransactionDataList", "lenTransaction/mSecond", strconv.Itoa(lenTxData)+"/"+strconv.Itoa(int(mtsBatchTx)))
    // Determine the sync status.
    synced := false
    if daaScoreAvailable - vspcListNext[lenVspcNext-1].DaaScore < uint64(archiveLenVspcListMax+hysteresis) {
        synced = true
    }
    return synced, daaScoreAvailable, 0, vspcListNext, txDataList, nil
}

////////////////////////////////
func archiveCheckRollback(vspcListPrev []storage.DataVspcType, vspcListNext []storage.DataVspcType, daaScoreStart uint64) (uint64, []storage.DataVspcType) {
    if len(vspcListPrev) <= 1 {
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
func archiveGetRuntime(key string) (string, string, string, error) {
    row := archiveRuntime.sessionCassa.Query(archiveCqlnGetRuntime, key)
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
func archiveGetRuntimeNodeSynced() (bool, error) {
    synced, _, _, err := archiveGetRuntime("ST_SYNCED")
    if err != nil {
        return false, err
    }
    if synced == "1" {
        return true, nil
    }
    return false, nil
}

////////////////////////////////
func archiveGetRuntimeChainBlockLast() (string, uint64, uint64, error) {
    hash, blueScore, daaScore, err := archiveGetRuntime("H_CBLOCK_LAST")
    if err != nil {
        return "", 0, 0, err
    }
    intBlueScore, _ := strconv.ParseUint(blueScore, 10, 64)
    intDaaScore, _ := strconv.ParseUint(daaScore, 10, 64)
    return hash, intBlueScore, intDaaScore, nil
}

////////////////////////////////
func archiveGetRuntimeSynced() (bool, uint64, error) {
    synced, err := archiveGetRuntimeNodeSynced()
    if err != nil {
        return false, 0, err
    }
    _, _, daaScore, err := archiveGetRuntimeChainBlockLast()
    if err != nil {
        return false, 0, err
    }
    return synced, daaScore, nil
}

////////////////////////////////
func archiveGetNodeBlockTimestampMap(timestampMap map[string]uint64) (int64, error) {
    lenBlock := len(timestampMap)
    hashList := make([]string, 0, lenBlock)
    for hash := range timestampMap {
        hashList = append(hashList, hash)
    }
    mutex := new(sync.RWMutex)
    mtsBatch, err := archiveStartQueryBatchIn(lenBlock, func(iStart int, iEnd int, session *gocql.Session) (error) {
        hashIn := []string{}
        for i := iStart; i < iEnd; i ++ {
            hashIn = append(hashIn, "'"+hashList[i]+"'")
        }
        cql := strings.Replace(archiveCqlnGetBlockHeader, "{hashIn}", strings.Join(hashIn,","), 1)
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
func archiveGetNodeVspcList(daaScoreStart uint64, lenBlock int) ([]storage.DataVspcType, int64, error) {
    vspcMap := make(map[uint64]*storage.DataVspcType, lenBlock)
    timestampMap := make(map[string]uint64, lenBlock)
    mutex := new(sync.RWMutex)
    mtsBatch, err := archiveStartQueryBatchIn(lenBlock, func(iStart int, iEnd int, session *gocql.Session) (error) {
        daaScoreList := []string{}
        for i := iStart; i < iEnd; i ++ {
            daaScoreList = append(daaScoreList, strconv.FormatUint(daaScoreStart+uint64(i),10))
        }
        daaScoreIn := strings.Join(daaScoreList, ",")
        cql := strings.Replace(archiveCqlnGetVspcData, "{daascoreIn}", daaScoreIn, 1)
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
            vspcMap[daaScore] = &storage.DataVspcType{
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
    _, err = archiveGetNodeBlockTimestampMap(timestampMap)
    vspcList := make([]storage.DataVspcType, 0, lenBlock)
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
func archiveGetNodeTransactionDataMap(txDataList []storage.DataTransactionType) (map[string]*protowire.RpcTransaction, int64, error) {
    txDataMap := map[string]*protowire.RpcTransaction{}
    mutex := new(sync.RWMutex)
    mtsBatch, err := archiveStartQueryBatchIn(len(txDataList), func(iStart int, iEnd int, session *gocql.Session) (error) {
        txIdList := []string{}
        for i := iStart; i < iEnd; i ++ {
            txIdList = append(txIdList, "'"+txDataList[i].TxId+"'")
        }
        txIdIn := strings.Join(txIdList, ",")
        cql := strings.Replace(archiveCqlnGetTransactionData, "{txidIn}", txIdIn, 1)
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
func archiveGetNodeTransactionDataList(txDataList []storage.DataTransactionType) ([]storage.DataTransactionType, int64, error) {
    txDataMap, mtsBatch, err := archiveGetNodeTransactionDataMap(txDataList)
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

////////////////////////////////
func archiveGetNodeArchiveVspcTxDataList(daaScore string) (string, string, []string, map[string]string, error) {
    rowVspc := archiveRuntime.sessionCassa.Query(archiveCqlnGetVspcByDaaScore, daaScore)
    defer rowVspc.Release()
    var hash, txId string
    err := rowVspc.Scan(&hash, &txId)
    if err != nil {
        if err.Error() == "not found" {
            return "", "", nil, nil, nil
        }
        return "", "", nil, nil, err
    }
    rowBlock := archiveRuntime.sessionCassa.Query(archiveCqlnGetBlockByHash, hash)
    defer rowBlock.Release()
    var header string
    err = rowBlock.Scan(&header)
    if err != nil {
        return "", "", nil, nil, err
    }
    var txIdList []string
    if txId != "" {
        txIdList = strings.Split(txId, ",")
    } else {
        return hash, header, nil, nil, nil
    }
    intDaascore, _ := strconv.ParseUint(daaScore, 10, 64)
    if intDaascore < 110165000 {
        sort.Strings(txIdList)
    }
    lenTxId := len(txIdList)
    txDataMap := make(map[string]string, lenTxId)
    mutex := new(sync.RWMutex)
    _, err = archiveStartQueryBatchIn(lenTxId, func(iStart int, iEnd int, session *gocql.Session) (error) {
        txIdListIn := make([]string, 0, iEnd-iStart)
        for i := iStart; i < iEnd; i ++ {
            txIdListIn = append(txIdListIn, "'"+txIdList[i]+"'")
        }
        cql := strings.Replace(archiveCqlnGetTransactionData, "{txidIn}", strings.Join(txIdListIn,","), 1)
        row := session.Query(cql).Iter().Scanner()
        for row.Next() {
            var txId, data string
            err := row.Scan(&txId, &data)
            if err != nil {
                return err
            }
            mutex.Lock()
            txDataMap[txId] = data
            mutex.Unlock()
        }
        return row.Err()
    })
    if err != nil {
        return "", "", nil, nil, err
    }
    return hash, header, txIdList, txDataMap, nil
}

////////////////////////////////
func archiveGetNodeArchiveTxData(txId string) (string, error) {
    row := archiveRuntime.sessionCassa.Query(archiveCqlnGetTransactionByTxid, txId)
    defer row.Release()
    var data string
    err := row.Scan(&data)
    if err != nil {
        if err.Error() == "not found" {
            return "", nil
        }
        return "", err
    }
    return data, nil
}

////////////////////////////////
func archiveStartQueryBatchIn(lenBatch int, fQuery func(int, int, *gocql.Session) (error)) (int64, error) {
    if lenBatch <= 0 {
        return 0, nil
    }
    mtss := time.Now().UnixMilli()
    archiveMtsBatchLast = mtss
    nStart := 0
    nBatchAdj := archiveNumBatchMax
    mtsDelay := archiveMtsDelayQuery
    nRetry := 0
    for {
        nStartNext, err := archiveDoQueryBatchIn(lenBatch, nStart, fQuery, nBatchAdj)
        if err != nil {
            nRetry ++
            if nRetry > archiveNumBatchMax {
                return 0, err
            }
            nBatchAdj --
            if nBatchAdj < 1 {
                nBatchAdj = 1
            }
            mtsDelay = mtsDelay * (10+nRetry*2) / 10
            if mtsDelay > 1000 {
                mtsDelay = 1000
            }
        } else {
            nStart = nStartNext
            nRetry = 0
            nBatchAdj += 3
            if nBatchAdj > archiveNumBatchMax {
                nBatchAdj = archiveNumBatchMax
            }
            mtsDelay = mtsDelay * 8 / 10
            if mtsDelay < archiveMtsDelayQuery {
                mtsDelay = archiveMtsDelayQuery
            }
        }
        if nStart < 0 {
            break
        }
        mtsNow := time.Now().UnixMilli()
        if mtsNow - archiveMtsBatchLast >= 1000 {
            archiveMtsBatchLast = mtsNow
            slog.Debug("sequencer.archiveDoQueryBatchIn", "nStartNext", nStart)
        }
        time.Sleep(time.Duration(mtsDelay)*time.Millisecond)
    }
    return time.Now().UnixMilli() - mtss, nil
}

////////////////////////////////
func archiveDoQueryBatchIn(lenBatch int, nStart int, fQuery func(int, int, *gocql.Session) (error), nBatchAdj int) (int, error) {
    nBatch := int(math.Ceil(float64(lenBatch-nStart) / float64(archiveInMax)))
    nStartNext := -1
    if nBatch > archiveNumBatchMax {
        nBatch = archiveNumBatchMax
        nStartNext = archiveInMax * nBatch + nStart
    }
    wg := &sync.WaitGroup{}
    errList := make(chan error, nBatch)
    for i := 0; i < nBatch; i ++ {
        iStart := nStart + i*archiveInMax
        iEnd := nStart + (i+1)*archiveInMax
        if iEnd >= lenBatch {
            iEnd = lenBatch
        }
        wg.Add(1)
        go func() {
            err := fQuery(iStart, iEnd, archiveRuntime.sessionCassa)
            if err != nil {
                errList <- err
            }
            wg.Done()
        }()
    }
    wg.Wait()
    if len(errList) > 0 {
        err := <- errList
        return nStartNext, err
    }
    return nStartNext, nil
}
