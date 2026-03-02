
////////////////////////////////
package storage

//#include "rocksdb/c.h"
import "C"
import (
    "time"
    "log/slog"
    "math/rand"
    jsoniter "github.com/json-iterator/go"
    "kasplex-executor/config"
)

////////////////////////////////
var json = jsoniter.ConfigCompatibleWithStandardLibrary

////////////////////////////////
type runtimeType struct {
    cfgRocks config.RocksConfig
    rocks *C.rocksdb_transactiondb_t
    cfHandleList []*C.rocksdb_column_family_handle_t
    snapshot SnapshotType
}
var sRuntime runtimeType

////////////////////////////////
func Init(cfgRocks config.RocksConfig) {
    rand.Seed(time.Now().UnixNano())
    sRuntime.cfgRocks = cfgRocks
    initRocks()
    slog.Info("storage ready.")
}

////////////////////////////////
func Destroy() {
    releaseISD()
    destroyRocks()
    slog.Info("storage released.")
}
