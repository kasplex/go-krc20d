
////////////////////////////////
package storage

import (
    "fmt"
    // ...
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
        return 0, fmt.Errorf("preparing isd")
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
        }
    case snapshotREADY:
        if sRuntime.snapshot.Connected > 0 {
            sRuntime.snapshot.Status = snapshotINUSE
        } else if sRuntime.snapshot.Confirmed >= dtlSnapshot {
            releaseISD()
        }
    case snapshotINUSE:
        if sRuntime.snapshot.Connected <= 0 {
            sRuntime.snapshot.Connected = 0
            sRuntime.snapshot.Status = snapshotREADY
        }
    }
    return nil
}
