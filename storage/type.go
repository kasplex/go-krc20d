
////////////////////////////////
package storage

//#include "rocksdb/c.h"
import "C"
import (
    "sync"
    "kasplex-executor/protowire"
)

////////////////////////////////
type DataSyncedType struct {
    Synced bool `json:"synced,omitempty"`
    OpScore uint64 `json:"opscore,omitempty"`
    TxId string `json:"txid,omitempty"`
    Checkpoint string `json:"checkpoint,omitempty"`
    StCommitment string `json:"stcommitment,omitempty"`
    DaaScore uint64 `json:"daascore,omitempty"`
    Version string `json:"version,omitempty"`
}

////////////////////////////////
type DataVspcType struct {
    DaaScore uint64 `json:"daaScore,omitempty"`
    Hash string `json:"hash,omitempty"`
    Timestamp uint64 `json:"timestamp,omitempty"`
    SeqCommitment string `json:"seqcommitment,omitempty"`
    TxIdList []string `json:"-"`
}

////////////////////////////////
type DataTransactionType struct {
    TxId string
    DaaScore uint64
    BlockAccept string
    BlockTime uint64
    Data *protowire.RpcTransaction
}

////////////////////////////////
type DataOpStateType struct {
    BlockAccept string `json:"blockaccept,omitempty"`
    Fee uint64 `json:"fee,omitempty"`
    FeeLeast uint64 `json:"feeleast,omitempty"`
    MtsAdd int64 `json:"mtsadd,omitempty"`
    OpScore uint64 `json:"opscore,omitempty"`
    OpAccept int8 `json:"opaccept,omitempty"`
    OpError string `json:"operror,omitempty"`
    Checkpoint string `json:"checkpoint,omitempty"`
    StCommitment string `json:"stcommitment,omitempty"`
}

////////////////////////////////
type DataIndexOperationType struct {
    State *DataOpStateType `json:"state,omitempty"`
    Script map[string]string `json:"script,omitempty"`
    ScriptEx []map[string]string `json:"Scriptex,omitempty"`
    StBefore []string `json:"stbefore,omitempty"`
    StAfter []string `json:"stafter,omitempty"`
    TickAffc []string `json:"tickaffc,omitempty"`
    AddressAffc []string `json:"addressaffc,omitempty"`
    // ...
}

////////////////////////////////
type DataStatsType struct {
    TickAffcMap map[string]int
    AddressAffcMap map[string]map[string]string
    TickAffc []string
    AddressAffc []string
    // XxxAffc ...
}

////////////////////////////////
type DataKvRowType struct {  // {Key}={Val}
    P *[]byte `json:"-"`
    Key []byte `json:"k,omitempty"`
    Val []byte `json:"v,omitempty"`
}

////////////////////////////////
type DataOperationType struct {
    Block map[string]string
    Tx map[string]string
    TxInputs []map[string]string
    TxOutputs []map[string]string
    Op map[string]string
    OpScript []map[string]string
    OpKeyRules []map[string]string
    StBefore []string
    StAfter []string
    StRowBefore []*DataKvRowType
    StRowAfter []*DataKvRowType
    Checkpoint string
    StCommitment string
    SsInfo *DataStatsType
}

////////////////////////////////
type StateTokenMetaType struct {
    Max string `json:"max,omitempty"`
    Lim string `json:"lim,omitempty"`
    Pre string `json:"pre,omitempty"`
    Dec int `json:"dec,omitempty"`
    Mod string `json:"mod,omitempty"`
    From string `json:"from,omitempty"`
    To string `json:"to,omitempty"`
    Name string `json:"name,omitempty"`
    TxId string `json:"txid,omitempty"`
    OpAdd uint64 `json:"opadd,omitempty"`
    MtsAdd int64 `json:"mtsadd,omitempty"`
}

////////////////////////////////
type StateTokenType struct {
    Tick string `json:"tick,omitempty"`
    Max string `json:"max,omitempty"`
    Lim string `json:"lim,omitempty"`
    Pre string `json:"pre,omitempty"`
    Dec int `json:"dec,omitempty"`
    Mod string `json:"mod,omitempty"`
    From string `json:"from,omitempty"`
    To string `json:"to,omitempty"`
    Minted string `json:"minted,omitempty"`
    Burned string `json:"burned,omitempty"`
    Name string `json:"name,omitempty"`
    TxId string `json:"txid,omitempty"`
    OpAdd uint64 `json:"opadd,omitempty"`
    OpMod uint64 `json:"opmod,omitempty"`
    MtsAdd int64 `json:"mtsadd,omitempty"`
    MtsMod int64 `json:"mtsmod,omitempty"`
}

////////////////////////////////
type StateBalanceType struct {
    Address string `json:"address,omitempty"`
    Tick string `json:"tick,omitempty"`
    Dec int `json:"dec,omitempty"`
    Balance string `json:"balance,omitempty"`
    Locked string `json:"locked,omitempty"`
    OpMod uint64 `json:"opmod,omitempty"`
}

////////////////////////////////
type StateMarketType struct {
    Tick string `json:"tick,omitempty"`
    TAddr string `json:"taddr,omitempty"`
    UTxId string `json:"utxid,omitempty"`
    UAddr string `json:"uaddr,omitempty"`
    UAmt string `json:"uamt,omitempty"`
    UScript string `json:"uscript,omitempty"`
    TAmt string `json:"tamt,omitempty"`
    OpAdd uint64 `json:"opadd,omitempty"`
}

////////////////////////////////
type StateBlacklistType struct {
    Tick string `json:"tick,omitempty"`
    Address string `json:"address,omitempty"`
    OpAdd uint64 `json:"opadd,omitempty"`
}

////////////////////////////////
type StateContractType struct {
    Ca string `json:"ca,omitempty"`
    Op string `json:"op,omitempty"`
    Code []byte `json:"code,omitempty"`
    Bc []byte `json:"bc,omitempty"`
    BcSign string `json:"bcsign,omitempty"`
    OpMod uint64 `json:"opmod,omitempty"`
}

////////////////////////////////
type StateStatsOpCountType struct {
    Op string
    Count uint64
}
////////////////////////////////
type StateStatsType struct {
    OpTotal []StateStatsOpCountType `json:"optotal,omitempty"`
    OpTotalMap map[string]uint64 `json:"-"`
    FeeTotal uint64 `json:"feetotal,omitempty"`
    TokenTotal uint64 `json:"tokentotal,omitempty"`
    HolderTotal uint64 `json:"holdertotal,omitempty"`
    HolderTop [][2]string `json:"holdertop,omitempty"`
    OpMod uint64 `json:"opmod,omitempty"`
}

////////////////////////////////
// type StateXxx ...

////////////////////////////////
type DataStateMapType map[string]map[string]string

////////////////////////////////
type DataRollbackType struct {
    DaaScoreStart uint64 `json:"daascorestart,omitempty"`
    DaaScoreEnd uint64 `json:"daascoreend,omitempty"`
    CheckpointBefore string `json:"checkpointbefore,omitempty"`
    CheckpointAfter string `json:"checkpointafter,omitempty"`
    StCommitmentBefore string `json:"stcommitmentbefore,omitempty"`
    StCommitmentAfter string `json:"stcommitmentafter,omitempty"`
    OpScoreLast uint64 `json:"opscorelast,omitempty"`
    TxIdLast string `json:"txidlast,omitempty"`
    StRowMapBefore map[string]*DataKvRowType `json:"strowmapbefore,omitempty"`
    IddKeyList []string `json:"iddkeylist,omitempty"`
}

////////////////////////////////
/*type DataInputType struct {
    Hash string
    Index uint
    Amount uint64
}

////////////////////////////////
type DataFeeType struct {
    Txid string
    InputList []DataInputType
    AmountOut uint64
    Fee uint64
}*/

////////////////////////////////
const (
    snapshotEMPTY int = iota
    snapshotCREAT
    snapshotCONFM
    snapshotREADY
    snapshotINUSE
)

////////////////////////////////
type SnapshotType struct {
    sync.Mutex
    s *C.rocksdb_snapshot_t
    sn uint64
    Status int
    DaaScore uint64
    TxId string
    Checkpoint string
    Confirmed uint64
    Connected int
}

// ...
