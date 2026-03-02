////////////////////////////////
package api

import (
    "strconv"
    "github.com/gofiber/fiber/v2"
    "go-krc20d/storage"
)

////////////////////////////////
type v1resultOp struct {
    P string `json:"p"`
    Op string `json:"op"`
    Tick string `json:"tick,omitempty"`
    Ca string `json:"ca,omitempty"`
    Max string `json:"max,omitempty"`
    Lim string `json:"lim,omitempty"`
    Pre string `json:"pre,omitempty"`
    Dec string `json:"dec,omitempty"`
    Mod string `json:"mod,omitempty"`
    Daas string `json:"daas,omitempty"`
    Daae string `json:"daae,omitempty"`
    Amt string `json:"amt,omitempty"`
    From string `json:"from,omitempty"`
    To string `json:"to,omitempty"`
    Utxo string `json:"utxo,omitempty"`
    Price string `json:"price,omitempty"`
    Name string `json:"name,omitempty"`
    OpScore string `json:"opScore"`
    HashRev string `json:"hashRev"`
    FeeRev string `json:"feeRev"`
    TxAccept string `json:"txAccept"`
    OpAccept string `json:"opAccept"`
    OpError string `json:"opError"`
    Checkpoint string `json:"checkpoint"`
    MtsAdd string `json:"mtsAdd"`
    MtsMod string `json:"mtsMod"`
}

type v1responseOpList struct {
    Message string `json:"message"`
    Prev string `json:"prev,omitempty"`
    Next string `json:"next,omitempty"`
    Result []v1resultOp `json:"result"`
}

////////////////////////////////
func v1routeOpList(c *fiber.Ctx) (error) {
    r := &v1responseOpList{}
    _, synced, _, err := getInfoKRC20()
    if err != nil || !synced {
        r.Message = v1msgUnsynced
        return c.Status(403).JSON(r)
    }
    r.Message = v1msgSuccessful
    prev := c.Query("prev", "0")
    intPrev, _ := strconv.ParseUint(prev, 10, 64)
    next := c.Query("next", "9199999999999999999")
    intNext, _ := strconv.ParseUint(next, 10, 64)
    goPrev := false
    if intPrev > 0 {
        intNext = intPrev
        goPrev = true
    }
    address := c.Query("address", "")
    tick := c.Query("tick", "")
    ca := c.Query("ca", "")
    if address != "" {
        address, _ = filterAddress(address)
        if address == "" {
            r.Message = "address invalid"
            return c.Status(403).JSON(r)
        }
    }
    if ca != "" {
        tick = ca
    }
    if tick != "" {
        tick, _ = filterTickTxid(tick)
        if tick == "" {
            r.Message = "tick invalid"
            return c.Status(403).JSON(r)
        }
    }
    if (address == "" && tick == "") {
        r.Message = "empty parameters"
        return c.Status(403).JSON(r)
    }
    txIdList, err := storage.GetOpTxIdListByOpIndex(address, tick, intNext, goPrev)
    if err != nil {
        r.Message = v1msgInternalError
        return c.Status(403).JSON(r)
    }
    lenTxId := len(txIdList)
    r.Result = make([]v1resultOp, 0, lenTxId)
    if lenTxId == 0 {
        return c.JSON(r)
    }
    opDataMap, err := storage.GetOpDataMap(txIdList)
    if err != nil {
        r.Result = nil
        r.Message = v1msgInternalError
        return c.Status(403).JSON(r)
    }
    lenTxId = len(opDataMap)
    if lenTxId == 0 {
        return c.JSON(r)
    }
    v1FormatOpInfo(txIdList, opDataMap, r)
    r.Prev = r.Result[0].OpScore
    r.Next = r.Result[len(r.Result)-1].OpScore
    return c.JSON(r)
}

////////////////////////////////
func v1routeOpInfo(c *fiber.Ctx) (error) {
    r := &v1responseOpList{}
    _, synced, _, err := getInfoKRC20()
    if err != nil || !synced {
        r.Message = v1msgUnsynced
        return c.Status(403).JSON(r)
    }
    r.Message = v1msgSuccessful
    id := c.Params("id")
    opScore, _ := filterUint(id)
    txId, _ := filterHash(id)
    if (opScore <= 0 && txId == "") {
        r.Message = "op invalid"
        return c.Status(403).JSON(r)
    }
    if opScore > 0 {
        txId, err = storage.GetOpTxIdByOpScore(opScore)
        if err != nil {
            if err.Error() == v1msgDataExpired {
                r.Message = v1msgDataExpired
            } else {
                r.Message = v1msgInternalError
            }
            return c.Status(403).JSON(r)
        }
    }
    if txId == "" {
        r.Message = "op not found"
        return c.Status(403).JSON(r)
    }
    txIdList := []string{txId}
    r.Result = make([]v1resultOp, 0, 1)
    opDataMap, err := storage.GetOpDataMap(txIdList)
    if err != nil {
        r.Result = nil
        r.Message = v1msgInternalError
        return c.Status(403).JSON(r)
    }
    if opDataMap[txId] == nil {
        r.Result = nil
        r.Message = "op not found"
        return c.Status(403).JSON(r)
    }
    v1FormatOpInfo(txIdList, opDataMap, r)
    return c.JSON(r)
}

////////////////////////////////
func v1FormatOpInfo(txIdList []string, opDataMap map[string]*storage.DataIndexOperationType, output *v1responseOpList) {
    if output.Result == nil {
        output.Result = make([]v1resultOp, 0, len(txIdList))
    }
    for _, txId := range txIdList {
        opData := opDataMap[txId]
        result := v1resultOp{
            P: opData.Script["p"],
            Op: opData.Script["op"],
            OpScore: strconv.FormatUint(opData.State.OpScore, 10),
            HashRev: opData.TxId,
            FeeRev: strconv.FormatUint(opData.State.Fee, 10),
            TxAccept: "1",
            OpAccept: strconv.Itoa(int(opData.State.OpAccept)),
            OpError: opData.State.OpError,
            Checkpoint: opData.State.Checkpoint,
            MtsAdd: strconv.FormatInt(opData.State.MtsAdd, 10),
            MtsMod: strconv.FormatInt(opData.State.MtsAdd, 10),
            Tick: opData.Script["tick"],
            From: opData.Script["from"],
            To: opData.Script["to"],
            Name: opData.Script["name"],
        }
        if len(opData.Script["tick"]) == 64 {
            result.Ca = opData.Script["tick"]
            result.Tick = ""
        }
        if opData.Script["op"] == "deploy" {
            result.Max = opData.Script["max"]
            result.Lim = opData.Script["lim"]
            result.Pre = opData.Script["pre"]
            result.Dec = opData.Script["dec"]
            result.Mod = opData.Script["mod"]
        } else if (opData.Script["op"] == "mint" || opData.Script["op"] == "transfer" || opData.Script["op"] == "issue" || opData.Script["op"] == "burn") {
            result.Amt = opData.Script["amt"]
        } else if (opData.Script["op"] == "list") {
            result.Amt = opData.Script["amt"]
            result.Utxo = opData.Script["utxo"]
        } else if (opData.Script["op"] == "send") {
            result.Amt = opData.Script["amt"]
            result.Price = opData.Script["price"]
        } else if (opData.Script["op"] == "blacklist") {
            result.Mod = opData.Script["mod"]
        }
        output.Result = append(output.Result, result)
    }
}
