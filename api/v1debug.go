////////////////////////////////
package api

import (
    "time"
    "strconv"
    "github.com/gofiber/fiber/v2"
    "go-krc20d/storage"
)

////////////////////////////////
type v1resultDebugDatabase struct {
    Key string `json:"key,omitempty"`
    Val string `json:"val,omitempty"`
}

type v1responseDebugDatabaseList struct {
    Message string `json:"message"`
    Result []v1resultDebugDatabase `json:"result"`
    UtsQuery int64 `json:"utsQuery,omitempty"`
}

////////////////////////////////
func v1DebugDatabaseSeek(c *fiber.Ctx) (error) {
    if !aRuntime.cfg.AllowDebug {
        return c.Status(404).SendString("api disabled")
    }
    r := &v1responseDebugDatabaseList{}
    r.Message = v1msgSuccessful
    cf := c.Params("cf")
    key := c.Query("key", "")
    dsc := c.Query("dsc", "")
    mod := c.Query("mod", "kv")
    maxCount, _ := strconv.Atoi(c.Query("max","50"))
    var keyList []string
    var valList []string
    var err error
    uts := time.Now().UnixMicro()
    if cf == "0" {
        keyList, valList, err = storage.SeekStateRaw(key, maxCount, dsc=="1", mod=="k")
    } else if cf == "1" {
        keyList, valList, err = storage.SeekIndexRaw(key, maxCount, dsc=="1", mod=="k")
    } else {
        r.Message = "cf invalid"
        return c.Status(403).JSON(r)
    }
    if err != nil {
        r.Message = v1msgInternalError
        return c.Status(403).JSON(r)
    }
    uts = time.Now().UnixMicro() - uts
    r.UtsQuery = uts
    r.Result = make([]v1resultDebugDatabase, 0, len(keyList))
    for i := range keyList {
        r.Result = append(r.Result, v1resultDebugDatabase{
            Key: keyList[i],
            Val: valList[i],
        })
    }
    return c.JSON(r)
}