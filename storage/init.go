
////////////////////////////////
package storage

//#include "rocksdb/c.h"
import "C"
import (
    "time"
    "log/slog"
    "math/rand"
    "github.com/gocql/gocql"
    "kasplex-executor/config"
)

////////////////////////////////
type runtimeType struct {
    cfgCassa config.CassaConfig
    cfgRocks config.RocksConfig
    cassa *gocql.ClusterConfig
    sessionCassa *gocql.Session
    rocks *C.rocksdb_transactiondb_t
    cfHandleList []*C.rocksdb_column_family_handle_t
}
var sRuntime runtimeType

////////////////////////////////
func Init(cfgCassa config.CassaConfig, cfgRocks config.RocksConfig) {
    rand.Seed(time.Now().UnixNano())
    sRuntime.cfgCassa = cfgCassa
    sRuntime.cfgRocks = cfgRocks
    slog.Info("storage.Init start.")
    initCassa()
    initRocks()
    slog.Info("storage ready.")
}

////////////////////////////////
func Destroy() {
    destroyCassa()
    destroyRocks()
    slog.Info("storage released.")
}
