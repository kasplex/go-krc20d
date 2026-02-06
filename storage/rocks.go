
////////////////////////////////
package storage

//#cgo CFLAGS: -I${SRCDIR}/rocksdb-10.4.2/include
//#cgo LDFLAGS: -L${SRCDIR}/rocksdb-10.4.2 -lrocksdb -lm -lz -lsnappy -lzstd -llz4 -lbz2 -lstdc++ -static
//#include "rocksdb/c.h"
//#include "./rocks.h"
import "C"
import (
    "fmt"
    "log"
    "sync"
    "time"
    "bytes"
    "unsafe"
    "runtime"
    "sync/atomic"
    "golang.org/x/sync/errgroup"
)

////////////////////////////////
const (
    cfState = iota
    cfIndex
)
const nGetRocks = 32

////////////////////////////////
var rOpt *C.rocksdb_readoptions_t
var wOpt *C.rocksdb_writeoptions_t
var txOpt *C.rocksdb_transaction_options_t
var dbOpt *C.rocksdb_options_t
var bbOpt *C.rocksdb_block_based_table_options_t
var bbFp *C.rocksdb_filterpolicy_t
var txDbOpt *C.rocksdb_transactiondb_options_t
var cfNameList = []string{"default", "index"}
var cfOptList []*C.rocksdb_options_t
var cFilterList []*C.rocksdb_compactionfilter_t
var valPool sync.Pool
var rOptPool sync.Pool

////////////////////////////////
func SetDaaScoreLastRocks(daaScore uint64) {
    atomic.StoreUint64((*uint64)(unsafe.Pointer(&C.rocks_daaScoreLatest)), daaScore)
}

////////////////////////////////
func GetDaaScoreLastRocks() uint64 {
    return atomic.LoadUint64((*uint64)(unsafe.Pointer(&C.rocks_daaScoreLatest)))
}

////////////////////////////////
func initRocks() {
    rOpt = C.rocksdb_readoptions_create()
    wOpt = C.rocksdb_writeoptions_create()
    txOpt = C.rocksdb_transaction_options_create()
    dbOpt = C.rocksdb_options_create()
    C.rocksdb_options_set_use_fsync(dbOpt, C.int(1))
    C.rocksdb_options_set_create_if_missing(dbOpt, C.uchar(1))
    C.rocksdb_options_set_create_missing_column_families(dbOpt, C.uchar(1))
    C.rocksdb_options_set_write_buffer_size(dbOpt, C.size_t(256*1024*1024))
    C.rocksdb_options_set_max_write_buffer_number(dbOpt, C.int(4))
    C.rocksdb_options_set_max_background_compactions(dbOpt, C.int(4))
    bbOpt = C.rocksdb_block_based_options_create()
    C.rocksdb_block_based_options_set_block_size(bbOpt, C.size_t(8*1024))
    C.rocksdb_block_based_options_set_cache_index_and_filter_blocks(bbOpt, C.uchar(1))
    bbFp = C.rocksdb_filterpolicy_create_bloom(C.double(10))
    C.rocksdb_block_based_options_set_filter_policy(bbOpt, bbFp)
    C.rocksdb_options_set_block_based_table_factory(dbOpt, bbOpt)
    txDbOpt = C.rocksdb_transactiondb_options_create()
    C.rocksdb_transactiondb_options_set_transaction_lock_timeout(txDbOpt, C.int64_t(100))
    lenCF := len(cfNameList)
    cfOptList = make([]*C.rocksdb_options_t, lenCF)
    cfOptList[cfState] = C.rocksdb_options_create()
    cfOptList[cfIndex] = C.rocksdb_options_create()
    cFilterList = make([]*C.rocksdb_compactionfilter_t, lenCF)
    if sRuntime.cfgRocks.DtlIndex > 0 {
        cFilterList[cfIndex] = C.rocks_newCompactionFilter(C.uint64_t(sRuntime.cfgRocks.DtlIndex))
        C.rocksdb_options_set_compaction_filter(cfOptList[cfIndex], cFilterList[cfIndex])
    }
    cfNameListC := make([]*C.char, lenCF)
    cfNameListC[cfState] = C.CString(cfNameList[cfState])
    cfNameListC[cfIndex] = C.CString(cfNameList[cfIndex])
    dbNameC := C.CString(sRuntime.cfgRocks.Path)
    sRuntime.cfHandleList = make([]*C.rocksdb_column_family_handle_t, lenCF)
    defer func() {
        C.free(unsafe.Pointer(cfNameListC[cfState]))
        C.free(unsafe.Pointer(cfNameListC[cfIndex]))
        C.free(unsafe.Pointer(dbNameC))
    }()
    var errC *C.char
    sRuntime.rocks = C.rocksdb_transactiondb_open_column_families(dbOpt, txDbOpt, dbNameC, C.int(lenCF), &cfNameListC[0], &cfOptList[0], &sRuntime.cfHandleList[0], &errC)
    if errC != nil {
        err := errRocks(errC)
        destroyRocks()
        log.Fatalln("storage.Init fatal: ", err)
    }
    valPool = sync.Pool{
        New: func() any {
            p := new([]byte)
            *p = make([]byte, 256)
            return p
        },
    }
    rOptPool = sync.Pool{
        New: func() any {
            p := C.rocksdb_readoptions_create()
            return p
        },
    }
}

////////////////////////////////
func valPoolGet(lenVal int) (*[]byte) {
    p := valPool.Get().(*[]byte)
    if cap(*p) < lenVal {
        *p = make([]byte, lenVal*2)
    }
    *p = (*p)[:lenVal]
    return p
}

////////////////////////////////
func valPoolPut(p *[]byte) () {
    if cap(*p) <= 4096 {
        valPool.Put(p)
    }
}

////////////////////////////////
func errRocks(errC *C.char) (error) {
    errString := C.GoString(errC)
    C.rocksdb_free(unsafe.Pointer(errC))
    return fmt.Errorf(errString)
}

////////////////////////////////
func destroyRocks() {
    if sRuntime.rocks != nil {
        C.rocksdb_transactiondb_close(sRuntime.rocks)
        sRuntime.rocks = nil
    }
    if dbOpt != nil {
        C.rocksdb_options_destroy(dbOpt)
        dbOpt = nil
    }
    if bbOpt != nil {
        C.rocksdb_block_based_options_destroy(bbOpt)
        bbOpt = nil
        bbFp = nil
    } else if bbFp != nil {
        C.rocksdb_filterpolicy_destroy(bbFp)
        bbFp = nil
    }
    if txDbOpt != nil {
        C.rocksdb_transactiondb_options_destroy(txDbOpt)
        txDbOpt = nil
    }
    for i := range cfOptList {
        if cfOptList[i] != nil {
            C.rocksdb_options_destroy(cfOptList[i])
            cfOptList[i] = nil
        }
        if cFilterList[i] != nil {
            C.rocksdb_compactionfilter_destroy(cFilterList[i])
            cFilterList[i] = nil
        }
    }
    if txOpt != nil {
        C.rocksdb_transaction_options_destroy(txOpt)
        txOpt = nil
    }
    if rOpt != nil {
        C.rocksdb_readoptions_destroy(rOpt)
        rOpt = nil
    }
    if wOpt != nil {
        C.rocksdb_writeoptions_destroy(wOpt)
        wOpt = nil
    }
    rOptPool.New = func() any {
        return nil
    }
    for {
        opt := rOptPool.Get()
        if opt == nil {
            break
        }
        C.rocksdb_readoptions_destroy(opt.(*C.rocksdb_readoptions_t))
    }
}

////////////////////////////////
func getCF(tx *C.rocksdb_transaction_t, cf int, key []byte, fGet func([]byte) (error)) ([]byte, error) {
    lenKey := len(key)
    if lenKey == 0 {
        return nil, nil
    }
    var lenValC C.size_t
    var errC *C.char
    var rowValC *C.char
    var rowData []byte
    if tx != nil {
        rowValC = C.rocksdb_transaction_get_cf(tx, rOpt, sRuntime.cfHandleList[cf], (*C.char)(unsafe.Pointer(&key[0])), C.size_t(lenKey), &lenValC, &errC)
    } else {
        rowValC = C.rocksdb_transactiondb_get_cf(sRuntime.rocks, rOpt, sRuntime.cfHandleList[cf], (*C.char)(unsafe.Pointer(&key[0])), C.size_t(lenKey), &lenValC, &errC)
    }
    runtime.KeepAlive(key)
    if errC != nil {
        return nil, errRocks(errC)
    }
    if rowValC == nil {
        return nil, nil
    }
    defer C.rocksdb_free(unsafe.Pointer(rowValC))
    lenVal := int(lenValC)
    if lenVal == 0 || cf > 0 && lenVal <= 8 {
        rowData = []byte{}
    } else {
        if cf > 0 {
            rowData = unsafe.Slice((*byte)(unsafe.Add(unsafe.Pointer(rowValC),8)), lenVal-8)
        } else {
            rowData = unsafe.Slice((*byte)(unsafe.Pointer(rowValC)), lenVal)
        }
    }
    if fGet != nil {
        err := fGet(rowData)
        return nil, err
    }
    data := make([]byte, len(rowData))
    copy(data, rowData)
    return data, nil
}

////////////////////////////////
func seekCF(tx *C.rocksdb_transaction_t, cf int, keyStart []byte, keyEnd []byte, maxCount int, dsc bool, snapshot *C.rocksdb_snapshot_t, fGet func(int, []byte, []byte) (bool, error)) (error) {
    lenKeyStart := len(keyStart)
    lenKeyEnd := len(keyEnd)
    if dsc && lenKeyEnd == 0 {
        return fmt.Errorf("nil end")
    }
    var keyStartC *C.char
    var keyEndC *C.char
    if lenKeyStart > 0 {
        keyStartC = (*C.char)(unsafe.Pointer(&keyStart[0]))
    }
    if lenKeyEnd > 0 {
        keyEndC = (*C.char)(unsafe.Pointer(&keyEnd[0]))
    }
    rOptSeek := rOptPool.Get().(*C.rocksdb_readoptions_t)
    if rOptSeek == nil {
        return fmt.Errorf("nil option")
    }
    defer rOptPool.Put(rOptSeek)
    C.rocksdb_readoptions_set_iterate_lower_bound(rOptSeek, nil, 0)
    C.rocksdb_readoptions_set_iterate_upper_bound(rOptSeek, nil, 0)
    if lenKeyStart > 0 {
        C.rocksdb_readoptions_set_iterate_lower_bound(rOptSeek, keyStartC, C.size_t(lenKeyStart))
    }
    if lenKeyEnd > 0 {
        C.rocksdb_readoptions_set_iterate_upper_bound(rOptSeek, keyEndC, C.size_t(lenKeyEnd))
    }
    C.rocksdb_readoptions_set_snapshot(rOptSeek, snapshot)
    if snapshot == nil {
        C.rocksdb_readoptions_set_fill_cache(rOptSeek, C.uchar(1))
    } else {
        C.rocksdb_readoptions_set_fill_cache(rOptSeek, C.uchar(0))
    }
    var iter *C.rocksdb_iterator_t
    if tx != nil {
        iter = C.rocksdb_transaction_create_iterator_cf(tx, rOptSeek, sRuntime.cfHandleList[cf])
    } else {
        iter = C.rocksdb_transactiondb_create_iterator_cf(sRuntime.rocks, rOptSeek, sRuntime.cfHandleList[cf])
    }
    if iter == nil {
        return fmt.Errorf("nil iter")
    }
    defer C.rocksdb_iter_destroy(iter)
    if dsc {
        C.rocksdb_iter_seek_for_prev(iter, keyEndC, C.size_t(lenKeyEnd))
    } else {
        C.rocksdb_iter_seek(iter, keyStartC, C.size_t(lenKeyStart))
    }
    var errC *C.char
    C.rocksdb_iter_get_error(iter, &errC)
    if errC != nil {
        return errRocks(errC)
    }
    i := 0
    for C.rocksdb_iter_valid(iter) != 0 {
        var key []byte
        var lenKeyC C.size_t
        keyC := C.rocksdb_iter_key(iter, &lenKeyC)
        lenKey := int(lenKeyC)
        if lenKey == 0 {
            return fmt.Errorf("nil key")
        }
        key = unsafe.Slice((*byte)(unsafe.Pointer(keyC)), lenKey)
        if i == 0 && dsc && bytes.Compare(key,keyEnd) >= 0 {
            C.rocksdb_iter_prev(iter)
            continue
        }
        var val []byte
        var lenValC C.size_t
        valC := C.rocksdb_iter_value(iter, &lenValC)
        lenVal := int(lenValC)
        if lenVal == 0 || cf > 0 && lenVal <= 8 {
            val = []byte{}
        } else {
            if cf > 0 {
                val = unsafe.Slice((*byte)(unsafe.Add(unsafe.Pointer(valC),8)), lenVal-8)
            } else {
                val = unsafe.Slice((*byte)(unsafe.Pointer(valC)), lenVal)
            }
        }
        goNext, err := fGet(i, key, val)
        if err != nil {
            return err
        }
        i ++
        if !goNext || maxCount > 0 && i >= maxCount {
            break
        }
        if dsc {
            C.rocksdb_iter_prev(iter)
        } else {
            C.rocksdb_iter_next(iter)
        }
    }
    C.rocksdb_iter_get_error(iter, &errC)
    runtime.KeepAlive(keyStart)
    runtime.KeepAlive(keyEnd)
    if errC != nil {
        return errRocks(errC)
    }
    return nil
}

////////////////////////////////
func putCF(tx *C.rocksdb_transaction_t, cf int, key []byte, val []byte, ds uint64) (error) {
    lenKey := len(key)
    if lenKey == 0 {
        return nil
    }
    lenVal := len(val)
    var data []byte
    if lenVal > 0 {
        if cf > 0 {
            lenVal += 8
            p := valPoolGet(lenVal)
            defer valPoolPut(p)
            data = (*p)[:lenVal]
            *(*uint64)(unsafe.Pointer(&data[0])) = ds
            copy(unsafe.Slice((*byte)(unsafe.Pointer(&data[8])),lenVal-8), val)
        } else {
            data = val
        }
    } else if cf > 0 {
        p := valPoolGet(8)
        defer valPoolPut(p)
        data = (*p)[:8]
        *(*uint64)(unsafe.Pointer(&data[0])) = ds
        lenVal = 8
    }
    var dataC *C.char
    if lenVal > 0 {
        dataC = (*C.char)(unsafe.Pointer(&data[0]))
    }
    var errC *C.char
    if tx != nil {
        C.rocksdb_transaction_put_cf(tx, sRuntime.cfHandleList[cf], (*C.char)(unsafe.Pointer(&key[0])), C.size_t(lenKey), dataC, C.size_t(lenVal), &errC)
    } else {
        C.rocksdb_transactiondb_put_cf(sRuntime.rocks, wOpt, sRuntime.cfHandleList[cf], (*C.char)(unsafe.Pointer(&key[0])), C.size_t(lenKey), dataC, C.size_t(lenVal), &errC)
    }
    runtime.KeepAlive(key)
    runtime.KeepAlive(data)
    if errC != nil {
        return errRocks(errC)
    }
    return nil
}

////////////////////////////////
func deleteCF(tx *C.rocksdb_transaction_t, cf int, key []byte) (error) {
    lenKey := len(key)
    if lenKey == 0 {
        return nil
    }
    var errC *C.char
    if tx != nil {
        C.rocksdb_transaction_delete_cf(tx, sRuntime.cfHandleList[cf], (*C.char)(unsafe.Pointer(&key[0])), C.size_t(lenKey), &errC)
    } else {
        C.rocksdb_transactiondb_delete_cf(sRuntime.rocks, wOpt, sRuntime.cfHandleList[cf], (*C.char)(unsafe.Pointer(&key[0])), C.size_t(lenKey), &errC)
    }
    runtime.KeepAlive(key)
    if errC != nil {
        return errRocks(errC)
    }
    return nil
}

////////////////////////////////
func txBegin() (*C.rocksdb_transaction_t) {
    return C.rocksdb_transaction_begin(sRuntime.rocks, wOpt, txOpt, nil)
}

////////////////////////////////
func txDestroy(tx *C.rocksdb_transaction_t) {
    C.rocksdb_transaction_destroy(tx)
}

////////////////////////////////
func txCommit(tx *C.rocksdb_transaction_t, rollback bool) (error) {
    var errC *C.char
    C.rocksdb_transaction_commit(tx, &errC)
    if errC == nil {
        txDestroy(tx)
        return nil
    }
    err := errRocks(errC)
    if rollback {
        txRollback(tx)
    } else {
        txDestroy(tx)
    }
    return err
}

////////////////////////////////
func txRollback(tx *C.rocksdb_transaction_t) (error) {
    var errC *C.char
    C.rocksdb_transaction_rollback(tx, &errC)
    if errC == nil {
        txDestroy(tx)
        return nil
    }
    err := errRocks(errC)
    txDestroy(tx)
    return err
}

////////////////////////////////
func CompactCF(cf int) {
    C.rocksdb_compact_range_cf(C.rocksdb_transactiondb_get_base_db(sRuntime.rocks), sRuntime.cfHandleList[cf], nil, 0, nil, 0)
}

////////////////////////////////
func doGetBatchCF(tx *C.rocksdb_transaction_t, cf int, keyList []string, fGet func(int, []byte) (error)) (int64, error) {
    lenKey := len(keyList)
    if lenKey == 0 {
        return 0, nil
    }
    mtss := time.Now().UnixMilli()
    nBatch := (lenKey + nGetRocks - 1) / nGetRocks
    g := &errgroup.Group{}
    for i := 0; i < nBatch; i++ {
        start := i * nGetRocks
        end := (i+1) * nGetRocks
        for j := start; j < end; j++ {
            if j >= lenKey {
                break
            }
            index := j
            key := []byte(keyList[index])
            g.Go(func() error {
                _, err := getCF(tx, cf, key, func(val []byte) (error) {
                    return fGet(index, val)
                })
                return err
            })
        }
        err := g.Wait()
        if err != nil {
            return 0, err
        }
    }
    return time.Now().UnixMilli() - mtss, nil
}

////////////////////////////////
func createSnapshot() (*C.rocksdb_snapshot_t, uint64) {
    snap := C.rocksdb_transactiondb_create_snapshot(sRuntime.rocks)
    return snap, uint64(C.rocksdb_snapshot_get_sequence_number(snap))
}

////////////////////////////////
func destroySnapshot(snap *C.rocksdb_snapshot_t) {
    if snap == nil {
        return
    }
    C.rocksdb_transactiondb_release_snapshot(sRuntime.rocks, snap)
}

////////////////////////////////
func RemoveAllDataRocks(reopen bool) {
    destroyRocks()
    var errC *C.char
    opt := C.rocksdb_options_create()
    dbNameC := C.CString(sRuntime.cfgRocks.Path)
    C.rocksdb_destroy_db(opt, dbNameC, &errC)
    C.free(unsafe.Pointer(dbNameC))
    if errC != nil {
        C.rocksdb_free(unsafe.Pointer(errC))
    }
    C.rocksdb_options_destroy(opt)
    if reopen {
        initRocks()
    }
}
