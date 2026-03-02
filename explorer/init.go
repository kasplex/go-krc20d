
////////////////////////////////
package explorer

import (
    "fmt"
    "sync"
    "time"
    "context"
    "strconv"
    "log/slog"
    jsoniter "github.com/json-iterator/go"
    "kasplex-executor/config"
    "kasplex-executor/storage"
    "kasplex-executor/sequencer"
    "kasplex-executor/operation"
)

////////////////////////////////
var json = jsoniter.ConfigCompatibleWithStandardLibrary

////////////////////////////////
type runtimeType struct {
    ctx context.Context
    wg *sync.WaitGroup
    cfg config.StartupConfig
    vspcList []storage.DataVspcType
    rollbackList []storage.DataRollbackType
    opScoreLast uint64
    txIdLast string
    synced bool
    testnet bool
}
var eRuntime runtimeType

// Genesis block and available daaScore range.
var blockGenesis = "8367cbf97b332c728f85b8fd60b51d4e61616d6a9d57e50dd40b636d22048ccf"
var daaScoreRange = [][2]uint64{
    {83441551, 83525600},
    {90090600, 18446744073709551615},
}

////////////////////////////////
//const lenVspcListMax = 1000
//const lenVspcCheck = 200
const lenVspcListRuntimeMax = 2400
const lenReorgDaaScoreMax = 864000

////////////////////////////////
func Init(ctx context.Context, wg *sync.WaitGroup, cfg config.StartupConfig, testnet bool) (error) {
    slog.Info("explorer.Init start.")
    var err error
    eRuntime.synced = false
    eRuntime.ctx = ctx
    eRuntime.wg = wg
    eRuntime.cfg = cfg
    eRuntime.testnet = testnet
    if eRuntime.cfg.Hysteresis < 0 {
        eRuntime.cfg.Hysteresis = 0
    } else if eRuntime.cfg.Hysteresis > 1000 {
        eRuntime.cfg.Hysteresis = 1000
    }
    if (!testnet || eRuntime.cfg.BlockGenesis == "") {
        eRuntime.cfg.BlockGenesis = blockGenesis
    }
    if (!testnet || len(eRuntime.cfg.DaaScoreRange) <= 0) {
        eRuntime.cfg.DaaScoreRange = daaScoreRange
    }
    err = initRuntime(true)
    if err != nil {
        return err
    }
    lenVspc := len(eRuntime.vspcList)
    lenRollback := len(eRuntime.rollbackList)
    if lenVspc == 0 && lenRollback == 0 && eRuntime.cfg.SeedISD != "" {
        err = runISD(eRuntime.cfg.SeedISD)
        if err != nil {
            cleanISD(false)
            return err
        }
        err = initRuntime(false)
        if err != nil {
            return err
        }
        lenVspc = len(eRuntime.vspcList)
        lenRollback = len(eRuntime.rollbackList)
    }
    if lenVspc > 0 && eRuntime.cfg.RollbackOnInit > 0 {
        daaScoreRollback := eRuntime.vspcList[lenVspc-1].DaaScore - eRuntime.cfg.RollbackOnInit
        mtsRollback, err := storage.RollbackExecutionBatch(daaScoreRollback)
        if err != nil {
            return err
        }
        err = initRuntime(false)
        if err != nil {
            return err
        }
        slog.Info("storage.RollbackExecutionBatch", "rollback", strconv.FormatUint(daaScoreRollback,10), "mSecond", strconv.Itoa(int(mtsRollback)))
        lenVspc = len(eRuntime.vspcList)
        lenRollback = len(eRuntime.rollbackList)
    }
    if lenVspc > 0 {
        vspcLast := eRuntime.vspcList[lenVspc-1]
        if eRuntime.cfg.CompactOnInit {
            slog.Info("storage.CompactCF", "daaScoreLast", vspcLast.DaaScore)
            storage.SetDaaScoreLastRocks(vspcLast.DaaScore)
            storage.CompactIndex()
        }
        slog.Info("explorer.Init", "lastVspcDaaScore", vspcLast.DaaScore, "lastVspcBlockHash", vspcLast.Hash)
    } else {
        slog.Info("explorer.Init", "lastVspcDaaScore", eRuntime.cfg.DaaScoreRange[0][0], "lastVspcBlockHash", "")
    }
    if eRuntime.cfg.CheckCommitment && lenRollback > 0 {
        stCommitment, err := storage.BuildFullStateCommitment()
        if err != nil {
            return err
        }
        if stCommitment != eRuntime.rollbackList[lenRollback-1].StCommitmentAfter {
            slog.Warn("storage.BuildFullStateCommitment mismatch.", "stCommitmentRebuild", stCommitment, "stCommitmentLast", eRuntime.rollbackList[lenRollback-1].StCommitmentAfter)
            return fmt.Errorf("state mismatch")
        }
        eRuntime.rollbackList[lenRollback-1].StCommitmentAfter = stCommitment
    }
    err = sequencer.Init(eRuntime.cfg.Sequencer, eRuntime.cfg.Hysteresis, eRuntime.cfg.DaaScoreRange)
    if err != nil {
        return err
    }
    slog.Info("explorer ready.")
    return nil
}

////////////////////////////////
func initRuntime(startup bool) (error) {
    var err error
    _, eRuntime.rollbackList, err = storage.GetRuntimeRollbackLast(1, nil)
    if err != nil {
        return err
    }
    _, eRuntime.vspcList, err = storage.GetRuntimeVspcLast(lenVspcListRuntimeMax)
    if err != nil {
        return err
    }
    indexRollback := len(eRuntime.rollbackList) - 1
    if indexRollback >= 0 {
        eRuntime.opScoreLast = eRuntime.rollbackList[indexRollback].OpScoreLast
        eRuntime.txIdLast = eRuntime.rollbackList[indexRollback].TxIdLast
    }
    stateContractMap := map[string]*storage.StateContractType{}
    if startup {
        
        // system stcontract (KRC-20) load / update runtime ...
        
        err = operation.InitLyncs(eRuntime.cfg.Lyncs, stateContractMap)
        if err != nil {
            return fmt.Errorf("InitLyncs: " + err.Error())
        }
    } else {
        
        // Lyncs load/check ...
        // Lyncs apply / update runtime ...
        
    }
    return nil
}

////////////////////////////////
func Run() {
    eRuntime.wg.Add(1)
    defer eRuntime.wg.Done()
loop:
    for {
        select {
            case <-eRuntime.ctx.Done():
                slog.Info("explorer.Scan stopped.")
                break loop
            default:
                scan()
                // Basic loop delay.
                time.Sleep(100*time.Millisecond)
        }
    }
}
