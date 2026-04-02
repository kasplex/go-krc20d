
////////////////////////////////
package sequencer

import (
    "fmt"
    jsoniter "github.com/json-iterator/go"
    "krc20d/config"
    "krc20d/protowire"
    "krc20d/storage"
)

////////////////////////////////
var json = jsoniter.ConfigCompatibleWithStandardLibrary

////////////////////////////////
var seqMode string
var hysteresis int
var daaScoreRange [][2]uint64

////////////////////////////////
var GetSyncStatus func() (bool, uint64, error)
var GetVspcTxDataList func([]storage.DataVspcType) (bool, uint64, uint64, []storage.DataVspcType, []storage.DataTransactionType, error)
var GetTxDataMap func([]storage.DataTransactionType) (map[string]*protowire.RpcTransaction, int64, error)
var GetArchiveVspcTxDataList func(string) (string, string, []string, map[string]string, error)
var GetArchiveTxData func(string) (string, error)

////////////////////////////////
func Init(cfg config.SequencerConfig, cfgHysteresis int, cfgDaaScoreRange [][2]uint64) (error) {
    var err error
    switch cfg.Mode {
    case "kaspad":
        err = kaspadInit(cfg.Kaspad)
    case "archive":
        err = archiveInit(cfg.Cassandra)
    default:
        err = fmt.Errorf("mode invalid")
    }
    if err != nil {
        return err
    }
    seqMode = cfg.Mode
    hysteresis = cfgHysteresis
    daaScoreRange = cfgDaaScoreRange
    return nil
}

////////////////////////////////
func Ready() (bool) {
    return (seqMode != "")
}

////////////////////////////////
func checkDaaScoreRange(daaScore uint64) (bool, uint64) {
    for _, dRange := range daaScoreRange {
        if daaScore < dRange[0] {
            return false, dRange[0]
        } else if (daaScore <= dRange[1]) {
            return true, daaScore
        }
    }
    return false, daaScore
}
