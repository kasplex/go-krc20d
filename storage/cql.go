
////////////////////////////////
package storage

var (
    ////////////////////////////
    cqlnInitTable = []string {
        // v3.01
    }
    ////////////////////////////
    cqlnGetRuntime = "SELECT * FROM runtime WHERE key=?;"
    ////////////////////////////
    cqlnGetVspcData = "SELECT daascore,hash,txid FROM vspc WHERE daascore IN ({daascoreIn});"
    cqlnGetVspcData2 = "SELECT daascore,hash,reorg,txid FROM vspc WHERE daascore IN ({daascoreIn});"
    cqlnGetVspcByDaaScore = "SELECT hash,txid FROM vspc WHERE daascore=?;"
    ////////////////////////////
    cqlnGetTransactionData = "SELECT txid,data FROM transaction WHERE txid IN ({txidIn});"
    cqlnGetTransactionByTxid = "SELECT data FROM transaction WHERE txid=?;"
    ////////////////////////////
    cqlnGetBlockHeader = "SELECT hash,header FROM block WHERE hash IN ({hashIn});"
    cqlnGetBlockByHash = "SELECT header,verbose FROM block WHERE hash=?;"
)
