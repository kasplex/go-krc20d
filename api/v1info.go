////////////////////////////////
package api

import (
    "fmt"
    "time"
    "strconv"
    "github.com/gofiber/fiber/v2"
    "krc20d/config"
    "krc20d/sequencer"
    "krc20d/storage"
)

////////////////////////////////
type v1resultInfo struct {
    available bool
    synced bool
    Version string `json:"version"`
    VersionApi string `json:"versionApi"`
    DaaScore string `json:"daaScore"`
    DaaScoreGap string `json:"daaScoreGap"`
    OpScore string `json:"opScore"`
    OpTotal string `json:"opTotal"`
    TokenTotal string `json:"tokenTotal"`
    FeeTotal string `json:"feeTotal"`
}

type v1responseInfo struct {
    Message string `json:"message"`
    Result *v1resultInfo `json:"result"`
}

////////////////////////////////
const cacheTimeoutInfo = 1000

////////////////////////////////
var dataInfo v1resultInfo
var cacheStateInfo cacheStateType

////////////////////////////////
func v1routeInfo(c *fiber.Ctx) (error) {
    r := &v1responseInfo{}
    _, _, info, err := getInfoKRC20()
    if err != nil {
        r.Message = v1msgInternalError
        return c.Status(403).JSON(r)
    }
    r.Result = info
    if !info.synced {
        r.Message = v1msgUnsynced
        return c.Status(403).JSON(r)
    }
    r.Message = v1msgSynced
    return c.JSON(r)
}

////////////////////////////////
func getInfoKRC20() (bool, bool, *v1resultInfo, error) {
    if !sequencer.Ready() {
        return false, false, nil, fmt.Errorf("api not ready")
    }
    info := &v1resultInfo{}
    mtsNow := time.Now().UnixMilli()
    cacheAvailable := true
    cacheStateInfo.RLock()
    if mtsNow - cacheStateInfo.mtsUpdate > cacheTimeoutInfo {
        cacheAvailable = false
    } else {
        *info = dataInfo
    }
    cacheStateInfo.RUnlock()
    var synced bool
    if cacheAvailable {
        if aRuntime.cfg.AllowUnsync {
            synced = true
        } else {
            synced = info.synced
        }
        return info.available, synced, info, nil
    }
    var err error
    var daaScore uint64
    var dataSynced *storage.DataSyncedType
    daaScoreGap := uint64(999)
    stKeyStatsKRC20 := storage.KeyPrefixStateStats + "_#KRC-20"
    stStatsMap := storage.DataStateMapType{stKeyStatsKRC20:nil}
    statsData := &storage.StateStatsType{}
    opTotal := uint64(0)
    cacheStateInfo.Lock()
    defer cacheStateInfo.Unlock()
    info.available, daaScore, err = sequencer.GetSyncStatus()
    if err != nil {
        return false, false, nil, err
    }
    if aRuntime.cfg.AllowUnsync {
        info.available = true
    }
    info.DaaScore = strconv.FormatUint(daaScore, 10)
    dataSynced, err = storage.GetRuntimeSynced()
    if err != nil {
        return false, false, nil, err
    }
    info.synced = dataSynced.Synced
    info.VersionApi = config.Version
    info.Version = dataSynced.Version
    info.OpScore = strconv.FormatUint(dataSynced.OpScore, 10)
    if  daaScore >= dataSynced.DaaScore {
        daaScoreGap = daaScore - dataSynced.DaaScore
    }
    info.DaaScoreGap = strconv.FormatUint(daaScoreGap, 10)
    if daaScoreGap > 99 {
        info.synced = false
    }
    _, err = storage.GetStateBatch(stStatsMap)
    if err != nil {
        return false, false, nil, err
    }
    if stStatsMap[stKeyStatsKRC20] == nil || stStatsMap[stKeyStatsKRC20]["data"] == "" {
        info.OpTotal = "0"
        info.TokenTotal = "0"
        info.FeeTotal = "0"
    } else {
        err = json.Unmarshal([]byte(stStatsMap[stKeyStatsKRC20]["data"]), statsData)
        if err != nil {
            return false, false, nil, err
        }
        for i := range statsData.OpTotal {
            if statsData.OpTotal[i].Op != "all" {
                continue
            }
            opTotal = statsData.OpTotal[i].Count
            break
        }
        info.OpTotal = strconv.FormatUint(opTotal, 10)
        info.TokenTotal = strconv.FormatUint(statsData.TokenTotal, 10)
        info.FeeTotal = strconv.FormatUint(statsData.FeeTotal, 10)
    }
    if aRuntime.cfg.AllowUnsync {
        synced = true
    } else {
        synced = info.synced
    }
    dataInfo = *info
    cacheStateInfo.mtsUpdate = time.Now().UnixMilli()
    return info.available, synced, info, nil
}
