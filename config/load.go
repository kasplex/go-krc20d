
////////////////////////////////
package config

import (
    "os"
    "log"
    jsoniter "github.com/json-iterator/go"
    //"github.com/jessevdk/go-flags"
)

////////////////////////////////
var json = jsoniter.ConfigCompatibleWithStandardLibrary

////////////////////////////////
type cmdConfig struct {
    
    // ...
    
}

////////////////////////////////
type StartupConfig struct {
    SeqMode string `json:"seqMode"`
    Hysteresis int `json:"hysteresis"`
    BlockGenesis string `json:"blockGenesis"`
    DaaScoreRange [][2]uint64 `json:"daaScoreRange"`
    SeedISD string `json:"seedISD"`
    FullISD bool `json:"fullISD"`
    RollbackOnInit uint64 `json:"rollbackOnInit"`
    CheckCommitment bool `json:"checkCommitment"`
    CompactOnInit bool `json:"-"`
    Sequencer SequencerConfig `json:"-"`
    Lyncs LyncsConfig `json:"-"`
}
type SequencerConfig struct {
    Mode string `json:"-"`
    Kaspad KaspadConfig `json:"-"`
    Cassandra CassaConfig `json:"-"`
}
type KaspadConfig struct {
    Grpc []string `json:"grpc"`
}
type CassaConfig struct {
    Host string `json:"host"`
    Port int `json:"port"`
    User string `json:"user"`
    Pass string `json:"pass"`
    Crt string `json:"crt"`
    Space string `json:"space"`
}
type RocksConfig struct {
    Path string `json:"path"`
    DtlIndex uint64 `json:"dtlIndex"`
    DtlFailed uint64 `json:"dtlFailed"`
    IndexDisabled bool `json:"indexDisabled"`
    CompactOnInit bool `json:"compactOnInit"`
}
type LyncsConfig struct {
    NumSlot int `json:"numSlot"`
    MaxInSlot int `json:"maxInSlot"`
}
type ApiConfig struct {
    Host string `json:"host"`
    Port int `json:"port"`
    Timeout int `json:"timeout"`
    ConnMax int `json:"connMax"`
    PortISD int `json:"portISD"`
    ConnMaxISD int32 `json:"connMaxISD"`
    FullISD bool `json:"fullISD"`
    AllowUnsync bool `json:"allowUnsync"`
    AllowDebug bool `json:"allowDebug"`
}
type Config struct {
    Startup StartupConfig `json:"startup"`
    Kaspad KaspadConfig `json:"kaspad"`
    Cassandra CassaConfig `json:"cassandra"`
    Rocksdb RocksConfig `json:"rocksdb"`
    Lyncs LyncsConfig `json:"lyncs"`
    Api ApiConfig `json:"api"`
    Debug int `json:"debug"`
    Testnet bool `json:"testnet"`
}

////////////////////////////////
const Version = "3.01.260302"

////////////////////////////////
func Load(cfg *Config) {
    // File "config.json" should be in the same directory.
    dir, _ := os.Getwd()
    fp, err := os.Open(dir + "/config.json")
    if err == nil {
        defer fp.Close()
        jParser := json.NewDecoder(fp)
        err = jParser.Decode(&cfg)
        if err != nil {
            log.Fatalln("config.Load fatal:", err.Error())
        }
        cfg.Startup.Sequencer = SequencerConfig{
            Mode: cfg.Startup.SeqMode,
            Kaspad: cfg.Kaspad,
            Cassandra: cfg.Cassandra,
        }
        cfg.Startup.Lyncs = cfg.Lyncs
        cfg.Startup.CompactOnInit = cfg.Rocksdb.CompactOnInit
    }
    
    // use flags ...
    
}
