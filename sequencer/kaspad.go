
////////////////////////////////
package sequencer

import (
    "os"
    "fmt"
    "sync"
    "time"
    "context"
    "syscall"
    "strconv"
    "log/slog"
    "google.golang.org/grpc"
    "google.golang.org/grpc/credentials/insecure"
    "krc20d/config"
    "krc20d/protowire"
    "krc20d/storage"
)

////////////////////////////////
type kaspadRuntimeType struct {
    cfg config.KaspadConfig
    conn *grpc.ClientConn
    client protowire.RPCClient
    stream protowire.RPC_MessageStreamClient
    connRetry int
    indexGrpc int
    faultGrpc []uint8
}
var kaspadRuntime kaspadRuntimeType

////////////////////////////////
type kaspadSyncStatusCacheType struct {
    sync.RWMutex
    synced bool
    daaScore uint64
}
var kaspadSyncStatusCache kaspadSyncStatusCacheType

////////////////////////////////
type kaspadCacheDaaScoreType struct {
    Index []string
    DaaScore map[string]uint64
}
var kaspadCacheDaaScore kaspadCacheDaaScoreType

////////////////////////////////
const kaspadCacheDaaScoreMax = uint64(2048)

////////////////////////////////
func kaspadInit(cfg config.KaspadConfig) (error) {
    if len(cfg.Grpc) == 0 {
        return fmt.Errorf("config invalid")
    }
    kaspadRuntime.cfg = cfg
    kaspadRuntime.faultGrpc = make([]uint8, len(cfg.Grpc))
    kaspadConnect()
    var err error
    synced := false
    for i := 0; i < 37; i++ {
        synced, _, err = kaspadGetSyncStatus()
        if err != nil || !synced {
            slog.Warn("sequencer.kaspadGetSyncStatus failed/unsynced, retry after 3s.")
            time.Sleep(3000*time.Millisecond)
            continue
        }
    }
    if !synced {
        return fmt.Errorf("kaspad not ready")
    }
    kaspadCacheDaaScore.Index = make([]string, 0, kaspadCacheDaaScoreMax)
    kaspadCacheDaaScore.DaaScore = make(map[string]uint64, kaspadCacheDaaScoreMax)
    GetSyncStatus = kaspadGetSyncStatusCache
    GetVspcTxDataList = kaspadGetVspcTxDataList
    GetTxDataMap = kaspadGetTxDataMap
    GetArchiveVspcTxDataList = kaspadGetNodeArchiveVspcTxDataList
    GetArchiveTxData = kaspadGetNodeArchiveTxData
    return nil
}

////////////////////////////////
func kaspadGetVspcTxDataList(vspcList []storage.DataVspcType) (bool, uint64, uint64, []storage.DataVspcType, []storage.DataTransactionType, error) {
    // Determine the starting daaScore/hash.
    lenVspc := len(vspcList)
    daaScoreStart := vspcList[lenVspc-1].DaaScore
    hashStart := vspcList[lenVspc-1].Hash
    // Get the node sync status.
    synced, daaScoreAvailable, err := kaspadGetSyncStatus()
    if err != nil || !synced {
        slog.Warn("sequencer.kaspadGetSyncStatus failed/unsynced, sleep 3s.", "synced", synced, "error", err)
        time.Sleep(2700*time.Millisecond)
        return false, 0, 0, nil, nil, err
    }
    // Get the next vspc/tx data list.
    mtsBatchVspc := time.Now().UnixMilli()
    vspcData, err := kaspadGetVirtualChainFromBlockV2(hashStart)
    if err != nil {
        slog.Warn("sequencer.kaspadGetVirtualChainFromBlockV2 failed, sleep 1.35s.", "error", err.Error())
        time.Sleep(1050*time.Millisecond)
        return false, daaScoreAvailable, 0, nil, nil, err
    }
    // Check if a rollback is needed.
    lenRemoved := len(vspcData.RemovedChainBlockHashes)
    daaScoreRollback := uint64(0)
    if lenRemoved > 0 {
        for _, hashRemoved := range vspcData.RemovedChainBlockHashes {
            daaScoreRemoved, err := kaspadGetBlockDaaScore(hashRemoved)
            if err != nil {
                slog.Debug("sequencer.kaspadGetBlockDaaScore failed, sleep 0.75s.", "hashRemoved", hashRemoved)
                time.Sleep(450*time.Millisecond)
                return false, daaScoreAvailable, 0, nil, nil, err
            }
            if daaScoreRollback == 0 || daaScoreRemoved < daaScoreRollback {
                daaScoreRollback = daaScoreRemoved
            }
        }
    }
    if daaScoreRollback > 0 {
        return true, daaScoreAvailable, daaScoreRollback, nil, nil, nil
    }
    // Convert the vspc/tx data list.
    lenAdded := len(vspcData.AddedChainBlockHashes)
    if lenAdded == 0 || lenAdded != len(vspcData.ChainBlockAcceptedTransactions) {
        slog.Warn("sequencer.kaspadGetVirtualChainFromBlockV2 empty/mismatch, sleep 0.75s.")
        time.Sleep(450*time.Millisecond)
        return false, daaScoreAvailable, 0, nil, nil, fmt.Errorf("empty/mismatch vspc")
    }
    vspcListNext := make([]storage.DataVspcType, 0, lenAdded)
    txDataList := make([]storage.DataTransactionType, 0, 256)
    txIdMap := make(map[string]bool, 256)
    for i := range vspcData.ChainBlockAcceptedTransactions {
        daaScore := *vspcData.ChainBlockAcceptedTransactions[i].ChainBlockHeader.DaaScore
        if daaScore == 0 {
            slog.Warn("sequencer.kaspadGetVirtualChainFromBlockV2 zero daaScore, sleep 3s.")
            time.Sleep(2700*time.Millisecond)
            return false, daaScoreAvailable, 0, nil, nil, fmt.Errorf("zero daaScore")
        }
        if daaScore <= daaScoreStart {
            return true, daaScoreAvailable, daaScore, nil, nil, nil
        }
        hash := *vspcData.ChainBlockAcceptedTransactions[i].ChainBlockHeader.Hash
        timestamp := uint64(*vspcData.ChainBlockAcceptedTransactions[i].ChainBlockHeader.Timestamp)
        vspcListNext = append(vspcListNext, storage.DataVspcType{
            DaaScore: daaScore,
            Hash: hash,
            Timestamp: timestamp,
            TxIdList: []string{},
        })
        kaspadAddCacheBlockDaaScore(hash, daaScore)
        passed, _ := checkDaaScoreRange(daaScore)
        if !passed {
            continue
        }
        for j := range vspcData.ChainBlockAcceptedTransactions[i].AcceptedTransactions {
            txAccepted := vspcData.ChainBlockAcceptedTransactions[i].AcceptedTransactions[j]
            if txIdMap[*txAccepted.VerboseData.TransactionId] {
                slog.Warn("sequencer.kaspadGetVirtualChainFromBlockV2 duplicated, sleep 0.75s.", "daaScore", daaScore, "txId", *txAccepted.VerboseData.TransactionId)
                time.Sleep(450*time.Millisecond)
                return false, daaScoreAvailable, 0, nil, nil, fmt.Errorf("tx duplicated")
            }
            txData := &protowire.RpcTransaction{
                Version: *txAccepted.Version,  // FULL
                LockTime: *txAccepted.LockTime,  // FULL
                SubnetworkId: *txAccepted.SubnetworkId,  // FULL
                Gas: *txAccepted.Gas,  // FULL
                Payload: *txAccepted.Payload,
                Mass: *txAccepted.Mass,
                VerboseData: &protowire.RpcTransactionVerboseData{
                    TransactionId: *txAccepted.VerboseData.TransactionId,
                    Hash: *txAccepted.VerboseData.Hash,
                    ComputeMass: *txAccepted.VerboseData.ComputeMass,
                    BlockHash: *txAccepted.VerboseData.BlockHash,
                    BlockTime: *txAccepted.VerboseData.BlockTime,
                },
            }
            txData.Inputs = make([]*protowire.RpcTransactionInput, len(txAccepted.Inputs))
            for k := range txAccepted.Inputs {
                txData.Inputs[k] = &protowire.RpcTransactionInput{
                    PreviousOutpoint: &protowire.RpcOutpoint{
                        TransactionId: *txAccepted.Inputs[k].PreviousOutpoint.TransactionId,
                        Index: *txAccepted.Inputs[k].PreviousOutpoint.Index,
                    },
                    SignatureScript: *txAccepted.Inputs[k].SignatureScript,
                    Sequence: *txAccepted.Inputs[k].Sequence,
                    SigOpCount: *txAccepted.Inputs[k].SigOpCount,
                    VerboseData: &protowire.RpcTransactionInputVerboseData{
                        UtxoEntry: &protowire.RpcUtxoEntry{
                            Amount: *txAccepted.Inputs[k].VerboseData.UtxoEntry.Amount,
                            ScriptPublicKey: &protowire.RpcScriptPublicKey{
                                Version: txAccepted.Inputs[k].VerboseData.UtxoEntry.ScriptPublicKey.Version,
                                ScriptPublicKey: txAccepted.Inputs[k].VerboseData.UtxoEntry.ScriptPublicKey.ScriptPublicKey,
                            },
                            BlockDaaScore: *txAccepted.Inputs[k].VerboseData.UtxoEntry.BlockDaaScore,  // FULL
                            IsCoinbase: *txAccepted.Inputs[k].VerboseData.UtxoEntry.IsCoinbase,
                            VerboseData: &protowire.RpcUtxoEntryVerboseData{
                                ScriptPublicKeyType: *txAccepted.Inputs[k].VerboseData.UtxoEntry.VerboseData.ScriptPublicKeyType,
                                ScriptPublicKeyAddress: *txAccepted.Inputs[k].VerboseData.UtxoEntry.VerboseData.ScriptPublicKeyAddress,
                            },
                        },
                    },
                }
            }
            txData.Outputs = make([]*protowire.RpcTransactionOutput, len(txAccepted.Outputs))
            for k := range txAccepted.Outputs {
                txData.Outputs[k] = &protowire.RpcTransactionOutput{
                    Amount: *txAccepted.Outputs[k].Amount,
                    ScriptPublicKey: &protowire.RpcScriptPublicKey{
                        Version: txAccepted.Outputs[k].ScriptPublicKey.Version,
                        ScriptPublicKey: txAccepted.Outputs[k].ScriptPublicKey.ScriptPublicKey,
                    },
                    VerboseData: &protowire.RpcTransactionOutputVerboseData{
                        ScriptPublicKeyType: *txAccepted.Outputs[k].VerboseData.ScriptPublicKeyType,
                        ScriptPublicKeyAddress: *txAccepted.Outputs[k].VerboseData.ScriptPublicKeyAddress,
                    },
                }
            }
            txDataList = append(txDataList, storage.DataTransactionType{
                TxId: *txAccepted.VerboseData.TransactionId,
                DaaScore: daaScore,
                BlockAccept: hash,
                BlockTime: timestamp,
                Data: txData,
            })
            txIdMap[*txAccepted.VerboseData.TransactionId] = true
        }
    }
    mtsBatchVspc = time.Now().UnixMilli() - mtsBatchVspc
    lenVspcNext := len(vspcListNext)
    if lenVspcNext == 0 {
        slog.Debug("sequencer.kaspadGetVspcTxDataList empty, sleep 0.75s.", "daaScore", daaScoreStart)
        time.Sleep(450*time.Millisecond)
        return false, daaScoreAvailable, 0, nil, nil, fmt.Errorf("nil vspc")
    }
    kaspadExpireCacheBlockDaaScore(vspcListNext[0].DaaScore)
    slog.Info("sequencer.kaspadGetVirtualChainFromBlockV2", "daaScoreAvailable", daaScoreAvailable, "daaScoreStart", daaScoreStart, "daaScoreCache", strconv.Itoa(len(kaspadCacheDaaScore.Index))+"/"+strconv.Itoa(len(kaspadCacheDaaScore.DaaScore)), "lenBlock/lenTransaction/mSecond", strconv.Itoa(lenVspcNext)+"/"+strconv.Itoa(len(txDataList))+"/"+strconv.Itoa(int(mtsBatchVspc)))
    // Determine the sync status.
    synced = false
    if daaScoreAvailable - vspcListNext[lenVspcNext-1].DaaScore < uint64(300+hysteresis) {
        synced = true
    }
    return synced, daaScoreAvailable, 0, vspcListNext, txDataList, nil
}

////////////////////////////////
func kaspadGetTxDataMap(txDataList []storage.DataTransactionType) (map[string]*protowire.RpcTransaction, int64, error) {
    txDataMap := map[string]*protowire.RpcTransaction{}
    return txDataMap, 0, nil
}

////////////////////////////////
func kaspadGetSyncStatus() (bool, uint64, error) {
    info, err := kaspadGetServerInfo()
    kaspadSyncStatusCache.Lock()
    defer kaspadSyncStatusCache.Unlock()
    if err != nil {
        kaspadSyncStatusCache.synced = false
        kaspadSyncStatusCache.daaScore = 0
        return false, 0, err
    }
    kaspadSyncStatusCache.synced = info.IsSynced
    kaspadSyncStatusCache.daaScore = info.VirtualDaaScore
    return info.IsSynced, info.VirtualDaaScore, nil
}

////////////////////////////////
func kaspadGetSyncStatusCache() (bool, uint64, error) {
    var synced bool
    var daaScore uint64
    kaspadSyncStatusCache.RLock()
    synced = kaspadSyncStatusCache.synced
    daaScore = kaspadSyncStatusCache.daaScore
    kaspadSyncStatusCache.RUnlock()
    return synced, daaScore, nil
}

////////////////////////////////
func kaspadGetBlockDaaScore(hash string) (uint64, error) {
    daaScore, exists := kaspadCacheDaaScore.DaaScore[hash]
    if exists && daaScore > 0 {
        return daaScore, nil
    }
    r, err := kaspadGetBlock(hash)
    if err != nil {
        return 0, err
    }
    daaScore = r.Block.Header.DaaScore
    slog.Info("sequencer.kaspadGetBlockDaaScore", "hash", hash, "daaScore", daaScore)
    if daaScore == 0 {
        return 0, fmt.Errorf("nil block")
    }
    kaspadAddCacheBlockDaaScore(hash, daaScore)
    return daaScore, nil
}

////////////////////////////////
func kaspadAddCacheBlockDaaScore(hash string, daaScore uint64) {
    if (hash == "" || daaScore == 0) {
        return
    }
    if kaspadCacheDaaScore.DaaScore[hash] == 0 {
        kaspadCacheDaaScore.DaaScore[hash] = daaScore
        kaspadCacheDaaScore.Index = append(kaspadCacheDaaScore.Index, hash)
    }
}

////////////////////////////////
func kaspadExpireCacheBlockDaaScore(daaScore uint64) {
    if daaScore <= kaspadCacheDaaScoreMax {
        return
    }
    daaScore -= kaspadCacheDaaScoreMax
    s := 0
    lenCache := len(kaspadCacheDaaScore.Index)
    for i := 0; i < lenCache; i++ {
        hash := kaspadCacheDaaScore.Index[i]
        if kaspadCacheDaaScore.DaaScore[hash] > daaScore {
            s = i
            break
        }
        delete(kaspadCacheDaaScore.DaaScore, hash)
    }
    if s > 0 {
        kaspadCacheDaaScore.Index = kaspadCacheDaaScore.Index[s:]
    }
}

////////////////////////////////
func kaspadGetNodeArchiveVspcTxDataList(daaScore string) (string, string, []string, map[string]string, error) {
    return "", "", nil, nil, fmt.Errorf("disabled")
}


////////////////////////////////
func kaspadGetNodeArchiveTxData(txId string) (string, error) {
    return "", fmt.Errorf("disabled")
}

////////////////////////////////
func kaspadConnect() {
    ctxTimeout, cancelTimeout := context.WithTimeout(context.Background(), 13*time.Second)
    defer cancelTimeout()
    if kaspadRuntime.connRetry > 55 {
        slog.Error("sequencer.kaspadConnect retries exceeded.")
        syscall.Kill(os.Getpid(), syscall.SIGTERM)
        time.Sleep(345*time.Millisecond)
        return
    }
    kaspadRuntime.connRetry ++
    for {
        if kaspadRuntime.indexGrpc >= len(kaspadRuntime.cfg.Grpc) {
            kaspadRuntime.indexGrpc = 0
        }
        if kaspadRuntime.faultGrpc[kaspadRuntime.indexGrpc] <= 0 {
            break
        }
        kaspadRuntime.faultGrpc[kaspadRuntime.indexGrpc] ++
        if kaspadRuntime.faultGrpc[kaspadRuntime.indexGrpc] > 3 {
            kaspadRuntime.faultGrpc[kaspadRuntime.indexGrpc] = 0
        }
        kaspadRuntime.indexGrpc ++
    }
    slog.Info("sequencer.kaspadConnect dialing ..", "index", kaspadRuntime.indexGrpc, "grpc", kaspadRuntime.cfg.Grpc[kaspadRuntime.indexGrpc])
    var err error
    kaspadRuntime.conn, err = grpc.DialContext(ctxTimeout, kaspadRuntime.cfg.Grpc[kaspadRuntime.indexGrpc], grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock(), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(512*1024*1024)))
    if err != nil {
        slog.Warn("sequencer.kaspadConnect failed, retry later.", "error", err.Error())
        kaspadRuntime.conn = nil
        kaspadRuntime.client = nil
        return
    }
    kaspadRuntime.client = protowire.NewRPCClient(kaspadRuntime.conn)
}

////////////////////////////////
func kaspadDisconnect() {
    kaspadRuntime.faultGrpc[kaspadRuntime.indexGrpc] ++
    kaspadRuntime.indexGrpc ++
    if kaspadRuntime.conn != nil {
        kaspadRuntime.conn.Close()
        kaspadRuntime.conn = nil
        kaspadRuntime.client = nil
    }
}

////////////////////////////////
func kaspadRequest(m *protowire.KaspadRequest) (*protowire.KaspadResponse, error) {
    err := fmt.Errorf("nil client")
    if kaspadRuntime.client == nil {
        kaspadDisconnect()
        kaspadConnect()
        return nil, err
    }
    ctxTimeout, cancelTimeout := context.WithTimeout(context.Background(), 37*time.Second)
    defer cancelTimeout()
    kaspadRuntime.stream, err = kaspadRuntime.client.MessageStream(ctxTimeout)
    if err != nil {
        kaspadDisconnect()
        kaspadConnect()
        return nil, err
    }
    err = kaspadRuntime.stream.Send(m)
    if err != nil {
        kaspadDisconnect()
        kaspadConnect()
        return nil, err
    }
    r, err := kaspadRuntime.stream.Recv()
    if err != nil {
        kaspadDisconnect()
        kaspadConnect()
        return nil, err
    }
    kaspadRuntime.connRetry = 0
    return r, nil
}

////////////////////////////////
func kaspadGetInfo() (*protowire.GetInfoResponseMessage, error) {
    r, err := kaspadRequest(&protowire.KaspadRequest{Id:0, Payload:&protowire.KaspadRequest_GetInfoRequest{GetInfoRequest:&protowire.GetInfoRequestMessage{}}})
    if err != nil {
        return nil, err
    }
    response := r.GetGetInfoResponse()
    if response == nil {
        return nil, fmt.Errorf("nil info")
    }
    return response, nil
}

////////////////////////////////
func kaspadGetServerInfo() (*protowire.GetServerInfoResponseMessage, error) {
    r, err := kaspadRequest(&protowire.KaspadRequest{Id:0, Payload:&protowire.KaspadRequest_GetServerInfoRequest{GetServerInfoRequest:&protowire.GetServerInfoRequestMessage{}}})
    if err != nil {
        return nil, err
    }
    response := r.GetGetServerInfoResponse()
    if response == nil {
        return nil, fmt.Errorf("nil serverInfo")
    }
    return response, nil
}

////////////////////////////////
func kaspadGetBlockDagInfo() (*protowire.GetBlockDagInfoResponseMessage, error) {
    r, err := kaspadRequest(&protowire.KaspadRequest{Id:0, Payload:&protowire.KaspadRequest_GetBlockDagInfoRequest{GetBlockDagInfoRequest:&protowire.GetBlockDagInfoRequestMessage{}}})
    if err != nil {
        return nil, err
    }
    response := r.GetGetBlockDagInfoResponse()
    if response == nil {
        return nil, fmt.Errorf("nil dagInfo")
    }
    return response, nil
}

////////////////////////////////
func kaspadGetBlock(hash string) (*protowire.GetBlockResponseMessage, error) {
    r, err := kaspadRequest(&protowire.KaspadRequest{Id:0, Payload:&protowire.KaspadRequest_GetBlockRequest{GetBlockRequest:&protowire.GetBlockRequestMessage{
        Hash: hash,
        IncludeTransactions: false,
    }}})
    if err != nil {
        return nil, err
    }
    response := r.GetGetBlockResponse()
    if response == nil {
        return nil, fmt.Errorf("nil block")
    }
    return response, nil
}

////////////////////////////////
func kaspadGetVirtualChainFromBlockV2(startHash string) (*protowire.GetVirtualChainFromBlockV2ResponseMessage, error) {
    level := protowire.RpcDataVerbosityLevel_FULL
    count := uint64(hysteresis)
    r, err := kaspadRequest(&protowire.KaspadRequest{Id:0, Payload:&protowire.KaspadRequest_GetVirtualChainFromBlockV2Request{GetVirtualChainFromBlockV2Request:&protowire.GetVirtualChainFromBlockV2RequestMessage{
        StartHash: startHash,
        DataVerbosityLevel: &level,
        MinConfirmationCount: &count,
    }}})
    if err != nil {
        return nil, err
    }
    response := r.GetGetVirtualChainFromBlockV2Response()
    if response == nil {
        return nil, fmt.Errorf("nil vspc")
    }
    return response, nil
}
