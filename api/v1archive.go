////////////////////////////////
package api

import (
    "strings"
    "strconv"
    "github.com/gofiber/fiber/v2"
    //"krc20d/sequencer"
    "krc20d/storage"
)

////////////////////////////////
type v1resultArchiveOpList struct {
    Opscore uint64 `json:"opscore"`
    Addressaffc string `json:"addressaffc,omitempty"`
    Script string `json:"script"`
    State string `json:"state"`
    Tickaffc string `json:"tickaffc,omitempty"`
    Txid string `json:"txid"`
}

type v1responseArchiveOpList struct {
    Message string `json:"message"`
    Result []v1resultArchiveOpList `json:"result"`
}

////////////////////////////////
type v1resultArchiveBlock struct {
    Hash string `json:"hash"`
    DaaScore uint64 `json:"daascore"`
    Header string `json:"header"`
    Verbose string `json:"verbose"`
}

type v1resultArchiveTransaction struct {
    Txid string `json:"txid"`
    Data string `json:"data"`
}

type v1resultArchiveVspc struct {
    ChainBlock *v1resultArchiveBlock `json:"chainBlock"`
    TxList []*v1resultArchiveTransaction `json:"txList"`
}

type v1responseArchiveVspc struct {
    Message string `json:"message"`
    Result []v1resultArchiveVspc `json:"result"`
}

type v1responseArchiveTransaction struct {
    Message string `json:"message"`
    Result *v1resultArchiveTransaction `json:"result"`
}

////////////////////////////////
const daaScoreHysteresis = uint64(170)

////////////////////////////////
func v1ArchiveOpList(c *fiber.Ctx) (error) {
    r := &v1responseArchiveOpList{}
    _, synced, info, err := getInfoKRC20()
    if err != nil || !synced {
        r.Message = v1msgUnsynced
        return c.Status(403).JSON(r)
    }
    r.Result = []v1resultArchiveOpList{}
    opRange := c.Params("oprange")
    opRange, _ = filterUintString(opRange)
    if opRange == "" {
        r.Message = "opRange invalid"
        return c.Status(403).JSON(r)
    }
    daaScoreLast, _ := strconv.ParseUint(info.DaaScore, 10, 64)
    daaScoreGap, _ := strconv.ParseUint(info.DaaScoreGap, 10, 64)
    daaScoreLast = daaScoreLast - daaScoreGap - daaScoreHysteresis
    intOpRange, _ := strconv.ParseUint(opRange, 10, 64)
    if intOpRange > daaScoreLast/10 {
        r.Message = "opRange " + v1msgNotReached
        return c.Status(403).JSON(r)
    }
    opList, err := storage.GetOpListByOpRange(opRange)
    if err != nil {
        if err.Error() == v1msgDataExpired {
            r.Message = v1msgDataExpired
        } else {
            r.Message = v1msgInternalError
        }
        return c.Status(403).JSON(r)
    }
    r.Result = make([]v1resultArchiveOpList, 0, len(opList))
    for i := range opList {
        if opList[i] == nil {
            continue
        }
        opList[i].State.StCommitment = ""
        state, _ := json.Marshal(opList[i].State)
        script, _ := json.Marshal(opList[i].Script)
        r.Result = append(r.Result, v1resultArchiveOpList{
            Opscore: opList[i].State.OpScore,
            Addressaffc: strings.Join(opList[i].AddressAffc, ","),
            Script: string(script),
            State: string(state),
            Tickaffc: strings.Join(opList[i].TickAffc, ","),
            Txid: opList[i].TxId,
        })
    }
    r.Message = v1msgSuccessful
    return c.JSON(r)
}

////////////////////////////////
func v1ArchiveVspc(c *fiber.Ctx) (error) {
    return c.Status(404).SendString("api disabled")
    /*if !sequencer.Ready() {
        return c.Status(404).SendString("api not ready")
    }
    r := &v1responseArchiveVspc{}
    available, _, info, err := getInfoKRC20()
    if err != nil || !available {
        r.Message = v1msgUnsynced
        return c.Status(403).JSON(r)
    }
    r.Result = []v1resultArchiveVspc{}
    daaScore, _ := filterUintString(c.Params("daascore"))
    if daaScore == "" {
        r.Message = "daaScore invalid"
        return c.Status(403).JSON(r)
    }
    intDaascore, _ := strconv.ParseUint(daaScore, 10, 64)
    daaScoreLast, _ := strconv.ParseUint(info.DaaScore, 10, 64)
    if intDaascore > daaScoreLast-daaScoreHysteresis {
        r.Message = "daaScore " + v1msgNotReached
        return c.Status(403).JSON(r)
    }
    hash, header, verbose, txIdList, txDataMap, err := storage.GetNodeArchiveVspcTxDataList(daaScore)  // ??
    if err != nil {
        r.Message = v1msgInternalError
        return c.Status(403).JSON(r)
    }
    r.Result = make([]v1resultArchiveVspc, 0, 1)
    if hash == "" {
        r.Message = v1msgSuccessful
        return c.JSON(r)
    }
    r.Result = append(r.Result, v1resultArchiveVspc{
        ChainBlock: &v1resultArchiveBlock{
            Hash: hash,
            DaaScore: intDaascore,
            Header: header,
            Verbose: verbose,
        },
        TxList: make([]*v1resultArchiveTransaction, 0, len(txIdList)),
    })
    for _, txId := range txIdList {
        r.Result[0].TxList = append(r.Result[0].TxList, &v1resultArchiveTransaction{
            Txid: txId,
            Data: txDataMap[txId],
        })
    }
    r.Message = v1msgSuccessful
    return c.JSON(r)*/
}

////////////////////////////////
func v1ArchiveTransaction(c *fiber.Ctx) (error) {
    return c.Status(404).SendString("api disabled")
    /*if !sequencer.Ready() {
        return c.Status(404).SendString("api not ready")
    }
    r := &v1responseArchiveTransaction{}
    txId, _ := filterHash(c.Params("id"))
    if txId == "" {
        r.Message = "txID invalid"
        return c.Status(403).JSON(r)
    }
    txData, err := storage.GetNodeArchiveTxData(txId)  // ??
    if err != nil {
        r.Message = v1msgInternalError
        return c.Status(403).JSON(r)
    }
    if txData == "" {
        r.Message = "tx not found"
        return c.Status(403).JSON(r)
    }
    r.Result = &v1resultArchiveTransaction{
        Txid: txId,
        Data: txData,
    }
    r.Message = v1msgSuccessful
    return c.JSON(r)*/
}
