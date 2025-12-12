
////////////////////////////////
package config

import (
    "os"
    "log"
    "encoding/json"
)

////////////////////////////////
type StartupConfig struct {
    Hysteresis int `json:"hysteresis"`
    DaaScoreRange [][2]uint64 `json:"daaScoreRange"`
    TickReserved []string `json:"tickReserved"`
    Lyncs LyncsConfig `json:"lyncs"`
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
    BtlIndex uint64 `json:"btlIndex"`
    BtlFailed uint64 `json:"btlFailed"`
    IndexDisabled bool `json:"indexDisabled"`
}
type LyncsConfig struct {
    NumSlot int `json:"numSlot"`
    MaxInSlot int `json:"maxInSlot"`
}
type Config struct {
    Startup StartupConfig `json:"startup"`
    Cassandra CassaConfig `json:"cassandra"`
    Rocksdb RocksConfig `json:"rocksdb"`
    Lyncs LyncsConfig `json:"lyncs"`
    Debug int `json:"debug"`
    Testnet bool `json:"testnet"`
}

////////////////////////////////
const Version = "3.01.251212"

////////////////////////////////
func Load(cfg *Config) {

    // File "config.json" should be in the same directory.
    
    dir, _ := os.Getwd()
    fp, err := os.Open(dir + "/config.json")
    if err != nil {
        log.Fatalln("config.Load fatal:", err.Error())
    }
    defer fp.Close()
    jParser := json.NewDecoder(fp)
    err = jParser.Decode(&cfg)
    if err != nil {
        log.Fatalln("config.Load fatal:", err.Error())
    }
    cfg.Startup.Lyncs = cfg.Lyncs
}
