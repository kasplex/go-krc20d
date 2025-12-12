
////////////////////////////////
package storage

import (
    "unsafe"
    "strconv"
    "strings"
    "encoding/json"
)

////////////////////////////////
func BuildDataKvRow(key []byte, val []byte) (*DataKvRowType) {
    if val == nil {
        val = []byte{}
    }
    lenKey := len(key)
    lenVal := len(val)
    raw := make([]byte, lenKey+1+lenVal)
    row := &DataKvRowType{
        P: &raw,
        Key: raw[:lenKey],
        Val: raw[lenKey+1:],
    }
    copy(row.Key, key)
    copy(row.Val, val)
    raw[lenKey] = 61
    return row
}

////////////////////////////////
func ConvIndexOpDataToKvRow(key string, opData *DataOperationType) (*DataKvRowType) {
    fee, _ := strconv.ParseUint(opData.Tx["fee"], 10, 64)
    FeeLeast, _ := strconv.ParseUint(opData.Op["feeLeast"], 10, 64)
    timestamp, _ := strconv.ParseInt(opData.Block["timestamp"], 10, 64)
    opScore, _ := strconv.ParseUint(opData.Op["score"], 10, 64)
    opAccept, _ := strconv.Atoi(opData.Op["accept"])
    state := &DataOpStateType{
        BlockAccept: opData.Block["hash"],
        Fee: fee,
        FeeLeast: FeeLeast,
        MtsAdd: timestamp,
        OpScore: opScore,
        OpAccept: int8(opAccept),
        OpError: opData.Op["error"],
        Checkpoint: opData.Checkpoint,
        StCommitment: opData.StCommitment,
    }
    data := &IndexOperationType{
        State: state,
        Script: opData.OpScript[0],
        StBefore: opData.StBefore,
        StAfter: opData.StAfter,
    }
    lenScript := len(opData.OpScript)
    if lenScript > 1 {
        data.ScriptEx = opData.OpScript[1:lenScript-1]
    }
    if opData.SsInfo != nil {
        data.TickAffc = opData.SsInfo.TickAffc
        data.AddressAffc = opData.SsInfo.AddressAffc
    }
    val, _ := json.Marshal(data)
    return BuildDataKvRow(unsafe.Slice(unsafe.StringData(key),len(key)), val)
}

////////////////////////////////
func ConvStateToKvRow(key string, data map[string]string) (*DataKvRowType) {
    lenKey := len(key)
    if data == nil || data["_key"] != "" && len(data) == 1 {
        return BuildDataKvRow(unsafe.Slice(unsafe.StringData(key),lenKey), nil)
    }
    var val []byte
    prefix := strings.SplitN(key, "_", 2)[0]
    if prefix == KeyPrefixStateToken {
        dec, _ := strconv.Atoi(data["dec"])
        opAdd, _ := strconv.ParseUint(data["opadd"], 10, 64)
        opMod, _ := strconv.ParseUint(data["opmod"], 10, 64)
        MtsAdd, _ := strconv.ParseInt(data["mtsadd"], 10, 64)
        MtsMod, _ := strconv.ParseInt(data["mtsmod"], 10, 64)
        val, _ = json.Marshal(&StateTokenType{
            Tick: data["tick"],
            Max: data["max"],
            Lim: data["lim"],
            Pre: data["pre"],
            Dec: dec,
            Mod: data["mod"],
            From: data["from"],
            To: data["to"],
            Minted: data["minted"],
            Burned: data["burned"],
            Name: data["name"],
            TxId: data["txid"],
            OpAdd: opAdd,
            OpMod: opMod,
            MtsAdd: MtsAdd,
            MtsMod: MtsMod,
        })
    } else if prefix == KeyPrefixStateBalance {
        dec, _ := strconv.Atoi(data["dec"])
        opMod, _ := strconv.ParseUint(data["opmod"], 10, 64)
        val, _ = json.Marshal(&StateBalanceType{
            Address: data["address"],
            Tick: data["tick"],
            Dec: dec,
            Balance: data["balance"],
            Locked: data["locked"],
            OpMod: opMod,
        })
    } else if prefix == KeyPrefixStateMarket {
        opAdd, _ := strconv.ParseUint(data["opadd"], 10, 64)
        val, _ = json.Marshal(&StateMarketType{
            Tick: data["tick"],
            TAddr: data["taddr"],
            UTxId: data["utxid"],
            UAddr: data["uaddr"],
            UAmt: data["uamt"],
            UScript: data["uscript"],
            TAmt: data["tamt"],
            OpAdd: opAdd,
        })
    } else if prefix == KeyPrefixStateBlacklist {
        opAdd, _ := strconv.ParseUint(data["opadd"], 10, 64)
        val, _ = json.Marshal(&StateBlacklistType{
            Tick: data["tick"],
            Address: data["address"],
            OpAdd: opAdd,
        })
    } else if prefix == KeyPrefixStateContract {
        opMod, _ := strconv.ParseUint(data["opmod"], 10, 64)
        val, _ = json.Marshal(&StateContractType{
            Ca: data["ca"],
            Op: data["op"],
            Code: []byte(data["code"]),
            Bc: []byte(data["bc"]),
            OpMod: opMod,
        })
    } else if prefix == KeyPrefixStateStats {
        val = unsafe.Slice(unsafe.StringData(data["data"]), len(data["data"]))
    } else {
        return nil
    }
    return BuildDataKvRow(unsafe.Slice(unsafe.StringData(key),lenKey), val)
}

////////////////////////////////
func ConvStateToStringMap(key string, val []byte) (map[string]string, error) {
    decoded := make(map[string]string, 8)
    prefix := strings.SplitN(key, "_", 2)[0]
    var err error
    if prefix == KeyPrefixStateToken {
        v2decoded := StateTokenType{}
        err = json.Unmarshal(val, &v2decoded)
        if err == nil {
            decoded["tick"] = v2decoded.Tick
            decoded["max"] = v2decoded.Max
            decoded["lim"] = v2decoded.Lim
            decoded["pre"] = v2decoded.Pre
            decoded["dec"] = strconv.Itoa(v2decoded.Dec)
            decoded["mod"] = v2decoded.Mod
            decoded["from"] = v2decoded.From
            decoded["to"] = v2decoded.To
            decoded["minted"] = v2decoded.Minted
            decoded["burned"] = v2decoded.Burned
            decoded["name"] = v2decoded.Name
            decoded["txid"] = v2decoded.TxId
            decoded["opadd"] = strconv.FormatUint(v2decoded.OpAdd, 10)
            decoded["opmod"] = strconv.FormatUint(v2decoded.OpMod, 10)
            decoded["mtsadd"] = strconv.FormatInt(v2decoded.MtsAdd, 10)
            decoded["mtsmod"] = strconv.FormatInt(v2decoded.MtsMod, 10)
        }
    } else if prefix == KeyPrefixStateBalance {
        v2decoded := StateBalanceType{}
        err = json.Unmarshal(val, &v2decoded)
        if err == nil {
            decoded["address"] = v2decoded.Address
            decoded["tick"] = v2decoded.Tick
            decoded["dec"] = strconv.Itoa(v2decoded.Dec)
            decoded["balance"] = v2decoded.Balance
            decoded["locked"] = v2decoded.Locked
            decoded["opmod"] = strconv.FormatUint(v2decoded.OpMod, 10)
        }
    } else if prefix == KeyPrefixStateMarket {
        v2decoded := StateMarketType{}
        err = json.Unmarshal(val, &v2decoded)
        if err == nil {
            decoded["tick"] = v2decoded.Tick
            decoded["taddr"] = v2decoded.TAddr
            decoded["utxid"] = v2decoded.UTxId
            decoded["uaddr"] = v2decoded.UAddr
            decoded["uamt"] = v2decoded.UAmt
            decoded["uscript"] = v2decoded.UScript
            decoded["tamt"] = v2decoded.TAmt
            decoded["opadd"] = strconv.FormatUint(v2decoded.OpAdd, 10)
        }
    } else if prefix == KeyPrefixStateBlacklist {
        v2decoded := StateBlacklistType{}
        err = json.Unmarshal(val, &v2decoded)
        if err == nil {
            decoded["tick"] = v2decoded.Tick
            decoded["address"] = v2decoded.Address
            decoded["opadd"] = strconv.FormatUint(v2decoded.OpAdd, 10)
        }
    } else if prefix == KeyPrefixStateContract {
        v2decoded := StateContractType{}
        err = json.Unmarshal(val, &v2decoded)
        if err == nil {
            decoded["ca"] = v2decoded.Ca
            decoded["op"] = v2decoded.Op
            decoded["code"] = string(v2decoded.Code)
            decoded["bc"] = string(v2decoded.Bc)
            decoded["opmod"] = strconv.FormatUint(v2decoded.OpMod, 10)
        }
    } else {
        decoded["data"] = string(val)
    }
    if err != nil {
        return nil, err
    }
    return decoded, nil
}
