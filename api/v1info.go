////////////////////////////////
package api

import (
    "time"
    "strconv"
    "github.com/gofiber/fiber/v2"
    "kasplex-executor/config"
	"kasplex-executor/storage"
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
const cacheTimeoutInfo = 5000

////////////////////////////////
var dataInfo v1resultInfo
var cacheStateInfo cacheStateType

////////////////////////////////
func v1routeInfo(c *fiber.Ctx) (error) {
    r := &v1responseInfo{}
    _, info, err := getInfoERC20()
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
func getInfoERC20() (bool, *v1resultInfo, error) {
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
    if cacheAvailable {
        return info.available, info, nil
    }
    var err error
    var daaScore uint64
    var dataSynced *storage.DataSyncedType
    daaScoreGap := uint64(999)
    stKeyStatsKRC20 := storage.KeyPrefixStateStats + "_#KRC-20"
    stateMap := storage.DataStateMapType{stKeyStatsKRC20:nil}
    stStatsMap := &storage.StateStatsType{}
    opTotal := uint64(0)
    cacheStateInfo.Lock()
    defer cacheStateInfo.Unlock()
    info.available, err = storage.GetRuntimeNodeSynced()
    if err != nil {
        return false, nil, err
    }
    _, _, daaScore, err = storage.GetRuntimeChainBlockLast()
    if err != nil {
        return false, nil, err
    }
    info.DaaScore = strconv.FormatUint(daaScore, 10)
    dataSynced, err = storage.GetRuntimeSynced()
    if err != nil {
        return false, nil, err
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
    _, err = storage.GetStateBatch(stateMap)
    if err != nil {
        return false, nil, err
    }
    if stateMap[stKeyStatsKRC20] == nil || stateMap[stKeyStatsKRC20]["data"] == "" {
        info.OpTotal = "0"
        info.TokenTotal = "0"
        info.FeeTotal = "0"
    } else {
        err = json.Unmarshal([]byte(stateMap[stKeyStatsKRC20]["data"]), stStatsMap)
        if err != nil {
            return false, nil, err
        }
        for i := range stStatsMap.OpTotal {
            if stStatsMap.OpTotal[i].Op != "all" {
                continue
            }
            opTotal = stStatsMap.OpTotal[i].Count
            break
        }
        info.OpTotal = strconv.FormatUint(opTotal, 10)
        info.TokenTotal = strconv.FormatUint(stStatsMap.TokenTotal, 10)
        info.FeeTotal = strconv.FormatUint(stStatsMap.FeeTotal, 10)
    }
    return info.available, info, nil
}
