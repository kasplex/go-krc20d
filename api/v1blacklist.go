////////////////////////////////
package api

import (
    "github.com/gofiber/fiber/v2"
    "kasplex-executor/storage"
)

////////////////////////////////
type v1resultBlackList struct {
    Ca string `json:"ca,omitempty"`
    Address string `json:"address"`
    OpScoreAdd string `json:"opScoreAdd"`
}

type v1responseBlackList struct {
    Message string `json:"message"`
    Prev string `json:"prev,omitempty"`
    Next string `json:"next,omitempty"`
    Result []v1resultBlackList `json:"result"`
}

////////////////////////////////
func v1routeBlackList(c *fiber.Ctx) (error) {
    r := &v1responseBlackList{}
    _, synced, _, err := getInfoKRC20()
    if err != nil || !synced {
        r.Message = v1msgUnsynced
        return c.Status(403).JSON(r)
    }
    r.Message = v1msgSuccessful
    tick, err := filterHash(c.Params("tick"))
    if tick == "" {
        r.Message = "ca invalid"
        return c.Status(403).JSON(r)
    }
    address := c.Query("address", "")
    if address != "" {
        address, _ = filterAddress(address)
        if address == "" {
            r.Message = "address invalid"
            return c.Status(403).JSON(r)
        }
        stBlacklist, err := storage.GetStateBlacklistData(tick, address)
        if err != nil {
            r.Message = v1msgInternalError
            return c.Status(403).JSON(r)
        }
        r.Result = make([]v1resultBlackList, 0, 1)
        if stBlacklist == nil {
            return c.JSON(r)
        }
        r.Result = append(r.Result, v1resultBlackList{
            Ca: stBlacklist["tick"],
            Address: stBlacklist["address"],
            OpScoreAdd: stBlacklist["opadd"],
        })
        return c.JSON(r)
    }
    prev := c.Query("prev", "")
    next := c.Query("next", "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz")
    goPrev := false
    if prev != "" {
        next = prev
        goPrev = true
    }
    stBlacklistList, err := storage.GetStateBlacklistList(tick, next, goPrev)
    if err != nil {
        r.Message = v1msgInternalError
        return c.Status(403).JSON(r)
    }
    lenBlacklist := len(stBlacklistList)
    r.Result = make([]v1resultBlackList, 0, lenBlacklist)
    if lenBlacklist == 0 {
        return c.JSON(r)
    }
    for i := range stBlacklistList {
        r.Result = append(r.Result, v1resultBlackList{
            Ca: stBlacklistList[i]["tick"],
            Address: stBlacklistList[i]["address"],
            OpScoreAdd: stBlacklistList[i]["opadd"],
        })
    }
    r.Prev = r.Result[0].Address
    r.Next = r.Result[lenBlacklist-1].Address
    return c.JSON(r)
}