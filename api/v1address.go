////////////////////////////////
package api

import (
    "github.com/gofiber/fiber/v2"
    "kasplex-executor/storage"
)

////////////////////////////////
type v1resultAddressToken struct {
    Tick string `json:"tick,omitempty"`
    Ca string `json:"ca,omitempty"`
    Balance string `json:"balance"`
    Locked string `json:"locked"`
    Dec string `json:"dec"`
    OpScoreMod string `json:"opScoreMod"`
}

type v1responseAddressTokenList struct {
    Message string `json:"message"`
    Prev string `json:"prev,omitempty"`
    Next string `json:"next,omitempty"`
    Result []v1resultAddressToken `json:"result"`
}

////////////////////////////////
func v1routeAddressTokenList(c *fiber.Ctx) (error) {
    r := &v1responseAddressTokenList{}
    _, synced, _, err := getInfoKRC20()
    if err != nil || !synced {
        r.Message = v1msgUnsynced
        return c.Status(403).JSON(r)
    }
    r.Message = v1msgSuccessful
    address, _ := filterAddress(c.Params("address"))
    if address == "" {
        r.Message = "address invalid"
        return c.Status(403).JSON(r)
    }
    prev := c.Query("prev", "")
    next := c.Query("next", "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")
    goPrev := false
    if prev != "" {
        next = prev
        goPrev = true
    }
    stBalanceList, err := storage.GetStateAddressBalanceList(address, next, goPrev)
    if err != nil {
        r.Message = v1msgInternalError
        return c.Status(403).JSON(r)
    }
    lenBalance := len(stBalanceList)
    r.Result = make([]v1resultAddressToken, 0, lenBalance)
    if lenBalance == 0 {
        return c.JSON(r)
    }
    for i := range stBalanceList {
        ca := ""
        tick := stBalanceList[i]["tick"]
        if len(tick) == 64 {
            ca = tick
            tick = ""
        }
        r.Result = append(r.Result, v1resultAddressToken{
            Tick: tick,
            Ca: ca,
            Balance: stBalanceList[i]["balance"],
            Locked: stBalanceList[i]["locked"],
            Dec: stBalanceList[i]["dec"],
            OpScoreMod: stBalanceList[i]["opmod"],
        })
    }
    r.Prev = r.Result[0].Tick
    r.Next = r.Result[lenBalance-1].Tick
    return c.JSON(r)
}

////////////////////////////////
func v1routeAddressTokenInfo(c *fiber.Ctx) (error) {
    r := &v1responseAddressTokenList{}
    _, synced, _, err := getInfoKRC20()
    if err != nil || !synced {
        r.Message = v1msgUnsynced
        return c.Status(403).JSON(r)
    }
    r.Message = v1msgSuccessful
    address, _ := filterAddress(c.Params("address"))
    if address == "" {
        r.Message = "address invalid"
        return c.Status(403).JSON(r)
    }
    tick, _ := filterTickTxid(c.Params("tick"))
    if tick == "" {
        r.Message = "tick invalid"
        return c.Status(403).JSON(r)
    }
    stBalance, err := storage.GetStateAddressBalanceData(address, tick)
    if err != nil {
        r.Message = v1msgInternalError
        return c.Status(403).JSON(r)
    }
    ca := ""
    tickShow := tick
    if len(tickShow) == 64 {
        ca = tickShow
        tickShow = ""
    }
    r.Result = make([]v1resultAddressToken, 0, 1)
    r.Result = append(r.Result, v1resultAddressToken{
        Tick: tickShow,
        Ca: ca,
        Balance: "0",
        Locked: "0",
        Dec: "0",
        OpScoreMod: "0",
    })
    if stBalance == nil {
        return c.JSON(r)
    }
    r.Result[0].Balance = stBalance["balance"]
    r.Result[0].Locked = stBalance["locked"]
    r.Result[0].Dec = stBalance["dec"]
    r.Result[0].OpScoreMod = stBalance["opmod"]
    return c.JSON(r)
}
