////////////////////////////////
package api

import (
    "github.com/gofiber/fiber/v2"
    "krc20d/storage"
)

////////////////////////////////
type v1resultMarket struct {
    Tick string `json:"tick,omitempty"`
    Ca string `json:"ca,omitempty"`
    From string `json:"from"`
    Amount string `json:"amount"`
    UtxId string `json:"uTxid"`
    UAddr string `json:"uAddr"`
    UAmt string `json:"uAmt"`
    UScript string `json:"uScript"`
    OpScoreAdd string `json:"opScoreAdd"`
}

type v1responseMarketList struct {
    Message string `json:"message"`
    Prev string `json:"prev,omitempty"`
    Next string `json:"next,omitempty"`
    Result []v1resultMarket `json:"result"`
}

////////////////////////////////
func v1routeMarketList(c *fiber.Ctx) (error) {
    r := &v1responseMarketList{}
    _, synced, _, err := getInfoKRC20()
    if err != nil || !synced {
        r.Message = v1msgUnsynced
        return c.Status(403).JSON(r)
    }
    r.Message = v1msgSuccessful
    tick, err := filterTickTxid(c.Params("tick"))
    if tick == "" {
        r.Message = "tick invalid"
        return c.Status(403).JSON(r)
    }
    ca := ""
    tickShow := tick
    if len(tickShow) == 64 {
        ca = tickShow
        tickShow = ""
    }
    address := c.Query("address", "")
    if address != "" {
        address, _ = filterAddress(address)
        if address == "" {
            r.Message = "address invalid"
            return c.Status(403).JSON(r)
        }
    }
    txId := c.Query("txid", "")
    if txId != "" {
        txId, _ = filterHash(txId)
        if txId == "" {
            r.Message = "txid invalid"
            return c.Status(403).JSON(r)
        }
    }
    if address != "" && txId != "" {
        stMarket, err := storage.GetStateMarketData(tick, address, txId)
        if err != nil {
            r.Message = v1msgInternalError
            return c.Status(403).JSON(r)
        }
        r.Result = make([]v1resultMarket, 0, 1)
        if stMarket == nil {
            return c.JSON(r)
        }
        r.Result = append(r.Result, v1resultMarket{
            Tick: tickShow,
            Ca: ca,
            From: stMarket["taddr"],
            Amount: stMarket["tamt"],
            UtxId: stMarket["utxid"],
            UAddr: stMarket["uaddr"],
            UAmt: stMarket["uamt"],
            UScript: stMarket["uscript"],
            OpScoreAdd: stMarket["opadd"],
        })
        return c.JSON(r)
    }
    prev := c.Query("prev", "")
    next := c.Query("next", "")
    goPrev := false
    if prev != "" {
        next = prev
        goPrev = true
    }
    stMarketList, err := storage.GetStateMarketList(tick, address, next, goPrev)
    if err != nil {
        r.Message = v1msgInternalError
        return c.Status(403).JSON(r)
    }
    lenMarket := len(stMarketList)
    r.Result = make([]v1resultMarket, 0, lenMarket)
    if lenMarket == 0 {
        return c.JSON(r)
    }
    for i := range stMarketList {
        r.Result = append(r.Result, v1resultMarket{
            Tick: tickShow,
            Ca: ca,
            From: stMarketList[i]["taddr"],
            Amount: stMarketList[i]["tamt"],
            UtxId: stMarketList[i]["utxid"],
            UAddr: stMarketList[i]["uaddr"],
            UAmt: stMarketList[i]["uamt"],
            UScript: stMarketList[i]["uscript"],
            OpScoreAdd: stMarketList[i]["opadd"],
        })
    }
    r.Prev = r.Result[0].From + "_" + r.Result[0].UtxId
    r.Next = r.Result[lenMarket-1].From + "_" + r.Result[lenMarket-1].UtxId
    return c.JSON(r)
}
