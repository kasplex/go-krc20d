
////////////////////////////////
package config

import (
    "os"
    "fmt"
    "log"
    "strings"
    "strconv"
    jsoniter "github.com/json-iterator/go"
    "github.com/jessevdk/go-flags"
)

////////////////////////////////
var json = jsoniter.ConfigCompatibleWithStandardLibrary

////////////////////////////////
const Version = "3.01.260403"
const HfDaaScore2026Q1 = 408300500

////////////////////////////////
type cmdConfig struct {
    // startup
    ConfigFile string `long:"configfile" description:"Use the specified configuration file; command-line arguments will be ignored."`
    ShowConfig bool `long:"showconfig" description:"Show all configuration parameters."`
    Sequencer string `long:"sequencer" description:"Sequencer type; \"kaspad\" or \"archive\"."`
    Hysteresis int `long:"hysteresis" description:"Number of DAA Scores hysteresis for data scanning."`
    LoopDelay int `long:"loopdelay" description:"Scan loop delay in milliseconds."`
    BlockGenesis string `long:"blockgenesis" description:"Genesis block hash."`
    DaaScoreRange string `long:"daascorerange" description:"Valid DAA Score range."`
    SeedISD string `long:"seedisd" description:"Seed URL for Initial State Download (ISD)."`
    FullISD bool `long:"fullisd" description:"Fully synchronize historical data (if supported by the ISD seed)."`
    RollbackOnInit uint64 `long:"rollbackoninit" description:"Number of DAA Scores to rollback on initialization."`
    CheckCommitment bool `long:"checkcommitment" description:"Check state commitment on initialization."`
    Debug int `long:"debug" description:"Debug information level; [0-3] available."`
    Testnet bool `long:"testnet" description:"Apply testnet parameters."`
    // kaspad
    KaspadGrpc string `long:"kaspad-grpc" description:"Kaspa node gRPC endpoint (comma-separated for multiple)."`
    // cassandra
    CassaHost string `long:"cassa-host" description:"Cassandra cluster host (comma-separated for multiple)."`
    CassaPort int `long:"cassa-port" description:"Cassandra cluster port."`
    CassaUser string `long:"cassa-user" description:"Cassandra cluster username."`
    CassaPass string `long:"cassa-pass" description:"Cassandra cluster password."`
    CassaCrt string `long:"cassa-crt" description:"Cassandra cluster SSL certificate."`
    CassaSpace string `long:"cassa-space" description:"Cassandra cluster keyspace name."`
    // rocksdb
    RocksPath string `long:"rocks-path" description:"RocksDB data path."`
    RocksDtlIndex uint64 `long:"rocks-dtl-index" description:"Maximum DAA Score lifetime for indexed data."`
    RocksDtlFailed uint64 `long:"rocks-dtl-failed" description:"Maximum DAA Score lifetime for indexed failed transactions."`
    RocksIndexDisabled bool `long:"rocks-indexdisabled" description:"Disable data indexing."`
    RocksCompactOnInit bool `long:"rocks-compactoninit" description:"Perform compaction on RocksDB initialization (may take a long time)."`
    // lyncs
    LyncsNumSlot int `long:"lyncs-numslot" description:"Number of parallel slots for the Lyncs engine."`
    LyncsMaxInSlot int `long:"lyncs-maxinslot" description:"Maximum number of tasks per slot for the Lyncs engine."`
    // Api
    ApiHost string `long:"api-host" description:"Listen host for the API server."`
    ApiPort int `long:"api-port" description:"Listen port for the API server."`
    ApiTimeout int `long:"api-timeout" description:"Processing timeout for the API server in seconds."`
    ApiConnMax int `long:"api-connmax" description:"Maximum number of concurrent connections for the API server."`
    ApiPortISD int `long:"api-port-isd" description:"Listen port for the ISD server."`
    ApiConnMaxISD int32 `long:"api-connmax-isd" description:"Maximum number of concurrent connections for the ISD server."`
    ApiFullISD bool `long:"api-fullisd" description:"Enable ISD server for full historical data."`
    ApiAllowUnsync bool `long:"api-allow-unsync" description:"Enable API server when not synchronized."`
    ApiAllowDebug bool `long:"api-allow-debug" description:"Enable debug API."`
}

////////////////////////////////
type StartupConfig struct {
    SeqMode string `json:"seqMode"`
    Hysteresis int `json:"hysteresis"`
    LoopDelay int `json:"loopDelay"`
    BlockGenesis string `json:"blockGenesis"`
    DaaScoreRange [][2]uint64 `json:"daaScoreRange"`
    SeedISD string `json:"seedISD"`
    FullISD bool `json:"fullISD"`
    RollbackOnInit uint64 `json:"rollbackOnInit"`
    CheckCommitment bool `json:"checkCommitment"`
    CompactOnInit bool `json:"compactOnInit"`
    Sequencer SequencerConfig `json:"sequencer"`
    Lyncs LyncsConfig `json:"lyncs"`
}
type SequencerConfig struct {
    Mode string `json:"mode"`
    Kaspad KaspadConfig `json:"kaspad"`
    Cassandra CassaConfig `json:"cassandra"`
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
    SeedISD string `json:"seedISD"`
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
var args = &cmdConfig{  // default
    Sequencer: "kaspad",
    Hysteresis: 3,
    LoopDelay: 550,
    SeedISD: "https://seed-krc20.kasplex.org",
    Debug: 2,
    KaspadGrpc: "127.0.0.1:16110",
    RocksPath: "./data",
    RocksDtlIndex: 86400000,
    RocksDtlFailed: 8640000,
    LyncsNumSlot: 8,
    LyncsMaxInSlot: 128,
    ApiHost: "0.0.0.0",
    ApiPort: 8005,
    ApiTimeout: 5,
    ApiConnMax: 500,
    ApiPortISD: 8006,
    ApiConnMaxISD: 4,
}

////////////////////////////////
var argsTestnet = &cmdConfig{  // default testnet
    BlockGenesis: "93b6a490ef1105e7e9cbc531dc051ba220afb566e16b2a9fcfed5d250712ec47",
    DaaScoreRange: "[[425179465,18446744073709551615]]",
    Sequencer: "kaspad",
    Hysteresis: 3,
    LoopDelay: 550,
    SeedISD: "https://seed-krc20-tn10.kasplex.org",
    Debug: 2,
    KaspadGrpc: "127.0.0.1:16210",
    RocksPath: "./data-tn10",
    RocksDtlIndex: 86400000,
    RocksDtlFailed: 8640000,
    LyncsNumSlot: 8,
    LyncsMaxInSlot: 128,
    ApiHost: "0.0.0.0",
    ApiPort: 8005,
    ApiTimeout: 5,
    ApiConnMax: 500,
    ApiPortISD: 8006,
    ApiConnMaxISD: 4,
}

////////////////////////////////
func Load(cfg *Config) {
    var err error
    parser := flags.NewParser(args, flags.Default)
    _, err = parser.Parse()
    if err != nil {
        errFlags, ok := err.(*flags.Error)
        if ok && errFlags.Type == flags.ErrHelp {
            os.Exit(0)
        }
        log.Fatalln("config.Load fatal:", err.Error())
    }
    if args.ShowConfig {
        defer func() {
            fmt.Println("")
            cfgStartup, _ := json.MarshalIndent(cfg.Startup, "", "    ")
            fmt.Println(`"startup": ` + string(cfgStartup))
            fmt.Println("")
            cfgRocksdb, _ := json.MarshalIndent(cfg.Rocksdb, "", "    ")
            fmt.Println(`"rocksdb": ` + string(cfgRocksdb))
            fmt.Println("")
            cfgApi, _ := json.MarshalIndent(cfg.Api, "", "    ")
            fmt.Println(`"api": ` + string(cfgApi))
            fmt.Println("")
            fmt.Println(`"debug": ` + strconv.Itoa(cfg.Debug))
            fmt.Println(`"testnet": ` + strconv.FormatBool(cfg.Testnet))
            fmt.Println("")
            os.Exit(0)
        }()
    }
    if args.ConfigFile != "" {
        if args.ConfigFile[:1] != "/" {
            dir, _ := os.Getwd()
            args.ConfigFile = dir + "/" + args.ConfigFile
        }
        fp, err := os.Open(args.ConfigFile)
        if err != nil {
            log.Fatalln("config.Load fatal:", err.Error())
        }
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
        cfg.Api.SeedISD = cfg.Startup.SeedISD
        return
    }
    if args.Testnet {
        parserTestnet := flags.NewParser(argsTestnet, flags.Default)
        _, err = parserTestnet.Parse()
        if err != nil {
            log.Fatalln("config.Load fatal:", err.Error())
        }
        args = argsTestnet
    }
    cfg.Startup.SeqMode = args.Sequencer
    cfg.Startup.Hysteresis = args.Hysteresis
    cfg.Startup.LoopDelay = args.LoopDelay
    cfg.Startup.BlockGenesis = args.BlockGenesis
    daaScoreRange := &[][2]uint64{}
    if args.DaaScoreRange != "" {
        err = json.Unmarshal([]byte(args.DaaScoreRange), daaScoreRange)
        if err == nil {
            cfg.Startup.DaaScoreRange = *daaScoreRange
        }
    }
    cfg.Startup.SeedISD = args.SeedISD
    cfg.Startup.FullISD = args.FullISD
    cfg.Startup.RollbackOnInit = args.RollbackOnInit
    cfg.Startup.CheckCommitment = args.CheckCommitment
    cfg.Startup.CompactOnInit = args.RocksCompactOnInit
    kaspadGrpc := []string{}
    if args.KaspadGrpc != "" {
        kaspadGrpc = strings.Split(args.KaspadGrpc, ",")
    }
    cfg.Startup.Sequencer = SequencerConfig{
        Mode: cfg.Startup.SeqMode,
        Kaspad: KaspadConfig{
            Grpc: kaspadGrpc,
        },
        Cassandra: CassaConfig{
            Host: args.CassaHost,
            Port: args.CassaPort,
            User: args.CassaUser,
            Pass: args.CassaPass,
            Crt: args.CassaCrt,
            Space: args.CassaSpace,
        },
    }
    cfg.Startup.Lyncs = LyncsConfig{
        NumSlot: args.LyncsNumSlot,
        MaxInSlot: args.LyncsMaxInSlot,
    }
    cfg.Rocksdb = RocksConfig{
        Path: args.RocksPath,
        DtlIndex: args.RocksDtlIndex,
        DtlFailed: args.RocksDtlFailed,
        IndexDisabled: args.RocksIndexDisabled,
    }
    cfg.Api = ApiConfig{
        Host: args.ApiHost,
        Port: args.ApiPort,
        Timeout: args.ApiTimeout,
        ConnMax: args.ApiConnMax,
        PortISD: args.ApiPortISD,
        ConnMaxISD: args.ApiConnMaxISD,
        FullISD: args.ApiFullISD,
        AllowUnsync: args.ApiAllowUnsync,
        AllowDebug: args.ApiAllowDebug,
        SeedISD: cfg.Startup.SeedISD,
    }
    cfg.Debug = args.Debug
    cfg.Testnet = args.Testnet
}
