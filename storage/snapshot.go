
////////////////////////////////
package storage

import (
    "fmt"
    "log/slog"
)

////////////////////////////////
const dtlSnapshot = 72000
const confirmSnapshot = 300

////////////////////////////////
func RequestISD() (uint64, uint64, error) {
    sRuntime.snapshot.Lock()
    defer sRuntime.snapshot.Unlock()
    if sRuntime.snapshot.Status < snapshotREADY {
        if sRuntime.snapshot.Status == snapshotEMPTY {
            sRuntime.snapshot.Status = snapshotCREAT
        }
        return 0, 0, fmt.Errorf("preparing")
    }
    sRuntime.snapshot.Connected ++
    return sRuntime.snapshot.sn, sRuntime.snapshot.DaaScore, nil
}

////////////////////////////////
func DisconnectISD() {
    sRuntime.snapshot.Lock()
    defer sRuntime.snapshot.Unlock()
    if sRuntime.snapshot.Connected <= 0 {
        sRuntime.snapshot.Connected = 0
        return
    }
    sRuntime.snapshot.Connected --
}

////////////////////////////////
func createISD(dataSynced *DataSyncedType) {
    sRuntime.snapshot.s, sRuntime.snapshot.sn = createSnapshot()
    sRuntime.snapshot.DaaScore = dataSynced.DaaScore
    sRuntime.snapshot.TxId = dataSynced.TxId
    sRuntime.snapshot.Checkpoint = dataSynced.Checkpoint
    sRuntime.snapshot.Confirmed = 0
    sRuntime.snapshot.Connected = 0
    sRuntime.snapshot.Status = snapshotCONFM
    slog.Info("storage.createISD", "status", "snapshotCONFM", "sn", sRuntime.snapshot.sn, "daaScore", sRuntime.snapshot.DaaScore)
}

////////////////////////////////
func releaseISD() {
    destroySnapshot(sRuntime.snapshot.s)
    sRuntime.snapshot.s = nil
    sRuntime.snapshot.sn = 0
    sRuntime.snapshot.DaaScore = 0
    sRuntime.snapshot.TxId = ""
    sRuntime.snapshot.Checkpoint = ""
    sRuntime.snapshot.Confirmed = 0
    sRuntime.snapshot.Connected = 0
    sRuntime.snapshot.Status = snapshotEMPTY
    slog.Info("storage.releaseISD", "status", "snapshotEMPTY")
}

////////////////////////////////
func ProcessISD(daaScoreRollback uint64) (error) {
    sRuntime.snapshot.Lock()
    defer sRuntime.snapshot.Unlock()
    dataSynced, err := GetRuntimeSynced()
    if err != nil {
        return err
    }
    if sRuntime.snapshot.DaaScore > 0 && dataSynced.DaaScore > sRuntime.snapshot.DaaScore {
        sRuntime.snapshot.Confirmed = dataSynced.DaaScore - sRuntime.snapshot.DaaScore
    }
    switch sRuntime.snapshot.Status {
    case snapshotCREAT:
        if !dataSynced.Synced{
            sRuntime.snapshot.Status = snapshotEMPTY
            return fmt.Errorf("unsynced")
        }
        createISD(dataSynced)
    case snapshotCONFM:
        if daaScoreRollback > 0 {
            if daaScoreRollback <= sRuntime.snapshot.DaaScore {
                releaseISD()
            }
        } else if sRuntime.snapshot.Confirmed > confirmSnapshot {
            sRuntime.snapshot.Status = snapshotREADY
            slog.Info("storage.ProcessISD", "status", "snapshotREADY", "confirmed", sRuntime.snapshot.Confirmed)
        }
    case snapshotREADY:
        if sRuntime.snapshot.Connected > 0 {
            sRuntime.snapshot.Status = snapshotINUSE
            slog.Info("storage.ProcessISD", "status", "snapshotINUSE", "connected", sRuntime.snapshot.Connected)
        } else if sRuntime.snapshot.Confirmed >= dtlSnapshot {
            releaseISD()
        }
    case snapshotINUSE:
        if sRuntime.snapshot.Connected <= 0 {
            sRuntime.snapshot.Connected = 0
            sRuntime.snapshot.Status = snapshotREADY
            slog.Info("storage.ProcessISD", "status", "snapshotREADY")
        }
    }
    return nil
}

////////////////////////////////
func SeekDataISD(step int, key []byte, pBuffer *[]byte, sizeMax int, fullISD bool) (int, []byte, error) {
    lenKey := len(key)
    keyStart := make([]byte, 0, lenKey+1)
    if lenKey > 0 {
        keyStart = append(keyStart, key...)
        keyStart = append(keyStart, ' ')
    }
    n := 0
    cf := 0
    var keyEnd []byte
    keyLast := make([]byte, 0, 128)
    for i := step; i < 4; i++ {
        switch i {
        case 0:
            cf = 0
            keyEnd = nil
        case 1:
            cf = 1
            if fullISD {
                keyEnd = nil
            } else {
                if len(keyStart) == 0 {
                    keyStart = []byte(keyPrefixRuntimeVspc+"_"+fmt.Sprintf("%020d",sRuntime.snapshot.DaaScore-36000))
                }
                keyEnd = []byte(keyPrefixRuntimeVspc+"`")
            }
        case 2:
            if fullISD {
                return i, nil, nil
            }
            cf = 1
            if len(keyStart) == 0 {
                keyStart = []byte(keyPrefixRuntimeRollback+"_"+fmt.Sprintf("%020d",sRuntime.snapshot.DaaScore-36000))
            }
            keyEnd = []byte(keyPrefixRuntimeRollback+"`")
        case 3:
            cf = 1
            if len(keyStart) == 0 {
                keyStart = []byte(keyPrefixRuntimeSynced)
            } else {
                return i, nil, nil
            }
            keyEnd = []byte(keyPrefixRuntimeSynced+"`")
        default:
            return 0, nil, fmt.Errorf("step invalid")
        }
        err := seekCF(nil, cf, keyStart, keyEnd, 0, false, sRuntime.snapshot.s, func(i int, key []byte, val []byte) (bool, error) {
            *pBuffer = append(*pBuffer, key...)
            *pBuffer = append(*pBuffer, 61)
            *pBuffer = append(*pBuffer, val...)
            *pBuffer = append(*pBuffer, 10)
            keyLast = keyLast[:0]
            keyLast = append(keyLast, key...)
            n ++
            if len(*pBuffer) >= sizeMax {
                return false, nil
            }
            return true, nil
        })
        if err != nil {
            return 0, nil, err
        }
        if n > 0 {
            return i, keyLast, nil
        } else {
            keyStart = keyStart[:0]
        }
    }
    return 0, nil, fmt.Errorf("step invalid")
}

////////////////////////////////
func SaveDataRowISD(cf int, row *DataKvRowType) (error) {
    if len(row.Val) == 0 {
        return nil
    }
    return putCF(nil, cf, row.Key, row.Val, 0)
}
