////////////////////////////////
package api

import (
    "strconv"
    "github.com/gofiber/fiber/v2"
    "kasplex-executor/storage"
)

////////////////////////////////
type v1resultDebugDatabase struct {
    Key string `json:"key"`
    Val string `json:"val"`
}

type v1responseDebugDatabaseList struct {
    Message string `json:"message"`
    Prev string `json:"prev,omitempty"`
    Next string `json:"next,omitempty"`
    Result []v1resultDebugDatabase `json:"result"`
}

////////////////////////////////
func v1DebugDatabaseSeek(c *fiber.Ctx) (error) {
    if !aRuntime.cfg.AllowDebug {
        return c.SendStatus(404)
    }
    r := &v1responseDebugDatabaseList{}
    r.Message = v1msgSuccessful
    cf := c.Params("cf")
    key := c.Query("key", "")
    dsc := c.Query("dsc", "")
    maxCount, _ := strconv.Atoi(c.Query("max","50"))
    var keyList []string
    var valList []string
    var err error
    if cf == "0" {
        keyList, valList, err = storage.SeekStateRaw(key, maxCount, dsc=="1")
    } else if cf == "1" {
        keyList, valList, err = storage.SeekIndexRaw(key, maxCount, dsc=="1")
    } else {
        r.Message = "cf invalid"
        return c.Status(403).JSON(r)
    }
    if err != nil {
        r.Message = v1msgInternalError
        return c.Status(403).JSON(r)
    }
    r.Result = make([]v1resultDebugDatabase, 0, len(keyList))
    for i := range keyList {
        r.Result = append(r.Result, v1resultDebugDatabase{
            Key: keyList[i],
            Val: valList[i],
        })
    }
    return c.JSON(r)
}