
////////////////////////////////
package storage

import (
    "fmt"
    //"log/slog"
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
    //slog.Info()..
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
    //slog.Info()..
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
        createISD(dataSynced)
    case snapshotCONFM:
        if daaScoreRollback > 0 {
            if daaScoreRollback <= sRuntime.snapshot.DaaScore {
                releaseISD()
            }
        } else if sRuntime.snapshot.Confirmed > confirmSnapshot {
            sRuntime.snapshot.Status = snapshotREADY
            //slog.Info()..
        }
    case snapshotREADY:
        if sRuntime.snapshot.Connected > 0 {
            sRuntime.snapshot.Status = snapshotINUSE
            //slog.Info()..
        } else if sRuntime.snapshot.Confirmed >= dtlSnapshot {
            releaseISD()
        }
    case snapshotINUSE:
        if sRuntime.snapshot.Connected <= 0 {
            sRuntime.snapshot.Connected = 0
            sRuntime.snapshot.Status = snapshotREADY
            //slog.Info()..
        }
    }
    return nil
}

////////////////////////////////
func SeekDataISD(cf int, key []byte, pBuffer *[]byte, sizeMax int) (int, []byte, error) {
    lenKey := len(key)
    keyStart := make([]byte, 0, lenKey+1)
    if lenKey > 0 {
        keyStart = append(keyStart, key...)
        keyStart = append(keyStart, ' ')
    }
    keyEnd := make([]byte, 0, 256)
    cfNew := cf
    n := 0
    for i := cf; i <= cfIndex; i++ {
        cfNew = i
        err := seekCF(nil, i, keyStart, nil, 0, false, func(i int, key []byte, val []byte) (bool, error) {
            *pBuffer = append(*pBuffer, key...)
            *pBuffer = append(*pBuffer, 61)
            *pBuffer = append(*pBuffer, val...)
            *pBuffer = append(*pBuffer, 10)
            keyEnd = keyEnd[:0]
            keyEnd = append(keyEnd, key...)
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
            break
        }
        keyStart = keyStart[:0]
    }
    return cfNew, keyEnd, nil
}
