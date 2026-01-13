////////////////////////////////
package api

import (
    "strings"
    "github.com/gofiber/fiber/v2"
	"kasplex-executor/storage"
)

////////////////////////////////
type v1resultArchiveOpList struct {
    Opscore uint64 `json:"opscore,omitempty"`
    Addressaffc string `json:"addressaffc,omitempty"`
    Script string `json:"script,omitempty"`
    State string `json:"state,omitempty"`
    Tickaffc string `json:"tickaffc,omitempty"`
    Txid string `json:"txid,omitempty"`
}

type v1responseArchiveOpList struct {
    Message string `json:"message"`
    Result []v1resultArchiveOpList `json:"result"`
}

////////////////////////////////
func v1ArchiveOpList(c *fiber.Ctx) (error) {
    r := &v1responseArchiveOpList{}
    opRange := c.Params("oprange")
	opRange, _ = filterUintString(opRange)
	if opRange == "" {
        r.Message = "opRange invalid"
		return c.Status(403).JSON(r)
	}
    opList, err := storage.GetOpListByOpRange(opRange)
    if err != nil {
        r.Message = v1msgInternalError
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

// ...
