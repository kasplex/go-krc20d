////////////////////////////////
package api

import (
    "strconv"
    "math/big"
    "github.com/gofiber/fiber/v2"
    "kasplex-executor/storage"
)

////////////////////////////////
type v1stateTokenHolder struct {
    Address string `json:"address"`
    Amount  string `json:"amount"`
}

type v1resultToken struct {
    Tick string `json:"tick,omitempty"`
    Ca string `json:"ca,omitempty"`
    Name string `json:"name,omitempty"`
    Max string `json:"max"`
    Lim string `json:"lim"`
    Pre string `json:"pre"`
    To string `json:"to"`
    Dec string `json:"dec"`
    Mod string `json:"mod"`
    Minted string `json:"minted"`
    Burned string `json:"burned"`
    OpScoreAdd string `json:"opScoreAdd"`
    OpScoreMod string `json:"opScoreMod"`
    State string `json:"state"`
    HashRev string `json:"hashRev"`
    MtsAdd string `json:"mtsAdd"`
    MtsMod string `json:"mtsMod"`
    HolderTotal string `json:"holderTotal,omitempty"`
    TransferTotal string `json:"transferTotal,omitempty"`
    MintTotal string `json:"mintTotal,omitempty"`
    OpCount []string `json:"opCount,omitempty"`
    FeeTotal string `json:"feeTotal,omitempty"`
    MaxSupply string `json:"maxSupply,omitempty"`
    TotalSupply string `json:"totalSupply,omitempty"`
    TotalBurned string `json:"totalBurned,omitempty"`
    Holder []v1stateTokenHolder `json:"holder,omitempty"`
}

type v1responseTokenList struct {
    Message string `json:"message"`
    Prev string `json:"prev,omitempty"`
    Next string `json:"next,omitempty"`
    Result []v1resultToken `json:"result"`
}

////////////////////////////////
func v1routeTokenList(c *fiber.Ctx) (error) {
    r := &v1responseTokenList{}
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
    tickList, err := storage.GetTickListByOpAdd(intNext, goPrev)
    if err != nil {
        r.Message = v1msgInternalError
        return c.Status(403).JSON(r)
    }
    lenTick := len(tickList)
    r.Result = make([]v1resultToken, 0, lenTick)
    if lenTick == 0 {
        return c.JSON(r)
    }
    stTokenMap, err := storage.GetStateTokenMap(tickList)
    if err != nil || len(stTokenMap) == 0 {
        r.Result = nil
        r.Message = v1msgInternalError
        return c.Status(403).JSON(r)
    }
    v1FormatTokenInfo(tickList, stTokenMap, r)
    r.Prev = r.Result[0].OpScoreAdd
    r.Next = r.Result[len(r.Result)-1].OpScoreAdd
    return c.JSON(r)
}

////////////////////////////////
func v1routeTokenInfo(c *fiber.Ctx) (error) {
    r := &v1responseTokenList{}
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
    r.Result = make([]v1resultToken, 0, 1)
    r.Result = append(r.Result, v1resultToken{
        Tick: tickShow,
        Ca: ca,
        Name: "",
        Max: "0",
        Lim: "0",
        Pre: "0",
        Dec: "0",
        Minted: "0",
        Burned: "0",
        OpScoreAdd: "0",
        OpScoreMod: "0",
    })
    if (err != nil && err.Error() == "ignored") {
        r.Result[0].State = "ignored"
        return c.JSON(r)
    }
    tickList := []string{tick}
    stTokenMap, err := storage.GetStateTokenMap(tickList)
    if err != nil {
        r.Result = nil
        r.Message = v1msgInternalError
        return c.Status(403).JSON(r)
    }
    if stTokenMap[storage.KeyPrefixStateToken+"_"+tick] == nil {
        r.Result[0].State = "unused"
        return c.JSON(r)
    }
    r.Result = r.Result[:0]
    v1FormatTokenInfo(tickList, stTokenMap, r)
    stBalanceBH, err := storage.GetStateAddressBalanceData(addressBH, tick)
    if stBalanceBH != nil && stBalanceBH["balance"] != "0" {
        burnedBig := new(big.Int)
        burnedBig.SetString(r.Result[0].Burned, 10)
        tmpBig := new(big.Int)
        tmpBig.SetString(stBalanceBH["balance"], 10)
        burnedBig = burnedBig.Add(burnedBig, tmpBig)
        r.Result[0].TotalBurned = burnedBig.Text(10)
        r.Result[0].TotalSupply = "0"
        r.Result[0].MaxSupply = "0"
        tmpBig.SetString(r.Result[0].Minted, 10)
        if tmpBig.Cmp(burnedBig) >= 0 {
            r.Result[0].TotalSupply = tmpBig.Sub(tmpBig, burnedBig).Text(10)
        }
        tmpBig.SetString(r.Result[0].Max, 10)
        if tmpBig.Cmp(burnedBig) >= 0 {
            r.Result[0].MaxSupply = tmpBig.Sub(tmpBig, burnedBig).Text(10)
        }
    } else {
        r.Result[0].TotalBurned = r.Result[0].Burned
        r.Result[0].TotalSupply = r.Result[0].Minted
        r.Result[0].MaxSupply = r.Result[0].Max
    }
    if r.Result[0].Max == "0" {
        r.Result[0].MaxSupply = "unlimited"
    }
    stats, err := storage.GetStateStatsData(tick)
    r.Result[0].TransferTotal = "0"
    r.Result[0].MintTotal = "0"
    if err != nil || stats == nil {
        r.Result[0].HolderTotal = "0"
        r.Result[0].OpCount = []string{}
        r.Result[0].FeeTotal = "0"
        r.Result[0].Holder = []v1stateTokenHolder{}
        return c.JSON(r)
    }
    r.Result[0].OpCount = make([]string, 0, 8)
    r.Result[0].Holder = make([]v1stateTokenHolder, 0, 100)
    r.Result[0].HolderTotal = strconv.FormatUint(stats.HolderTotal, 10)
    r.Result[0].FeeTotal = strconv.FormatUint(stats.FeeTotal, 10)
    for i := range stats.OpTotal {
        count := strconv.FormatUint(stats.OpTotal[i].Count, 10)
        r.Result[0].OpCount = append(r.Result[0].OpCount, stats.OpTotal[i].Op+":"+count)
        if stats.OpTotal[i].Op == "transfer" {
            r.Result[0].TransferTotal = count
        } else if stats.OpTotal[i].Op == "mint" {
            r.Result[0].MintTotal = count
        }
    }
    lenHolder := len(stats.HolderTop)
    if lenHolder > 100 {
        lenHolder = 100
    }
    for i := 0; i < lenHolder; i++ {
        r.Result[0].Holder = append(r.Result[0].Holder, v1stateTokenHolder{
            Address: stats.HolderTop[i][0],
            Amount: stats.HolderTop[i][1],
        })
    }
    return c.JSON(r)
}

////////////////////////////////
func v1FormatTokenInfo(tickList []string, stTokenMap storage.DataStateMapType, output *v1responseTokenList) {
    if output.Result == nil {
        output.Result = make([]v1resultToken, 0, len(tickList))
    }
    for _, tick := range tickList {
        stToken := stTokenMap[storage.KeyPrefixStateToken+"_"+tick]
        if stToken == nil {
            continue
        }
        state := "deployed"
        if stToken["max"] != "0" {
            maxBig := new(big.Int)
            maxBig.SetString(stToken["max"], 10)
            mintedBig := new(big.Int)
            mintedBig.SetString(stToken["minted"], 10)
            if mintedBig.Cmp(maxBig) >= 0 {
                state = "finished"
            }
        }
        if stToken["burned"] == "" {
            stToken["burned"] = "0"
        }
        ca := ""
        if stToken["mod"] != "issue" {
            stToken["mod"] = "mint"
        } else {
            ca = stToken["tick"]
            stToken["tick"] = ""
        }
        output.Result = append(output.Result, v1resultToken{
            Tick: stToken["tick"],
            Ca: ca,
            Name: stToken["name"],
            Max: stToken["max"],
            Lim: stToken["lim"],
            Pre: stToken["pre"],
            To: stToken["to"],
            Dec: stToken["dec"],
            Mod: stToken["mod"],
            Minted: stToken["minted"],
            Burned: stToken["burned"],
            OpScoreAdd: stToken["opadd"],
            OpScoreMod: stToken["opmod"],
            State: state,
            HashRev: stToken["txid"],
            MtsAdd: stToken["mtsadd"],
            MtsMod: stToken["mtsmod"],
        })
    }
}
