
////////////////////////////////
package explorer

import (
    "io"
    "fmt"
    "time"
    "sync"
    "bytes"
    "context"
    "strings"
    "net/http"
    "log/slog"
    "crypto/tls"
    "nhooyr.io/websocket"
    "github.com/kasplex/go-muhash"
    "go-krc20d/storage"
)

////////////////////////////////
func runISD(seedISD string) (error) {
    cleanISD(true)
    conn, err := dialISD(seedISD)
    if err != nil {
        return err
    }
    defer conn.Close(websocket.StatusNormalClosure, "")
    _writeWithTimeout := func (header *storage.IsdHeaderType) (error) {
        ctx, cancel := context.WithTimeout(context.Background(), 33*time.Second)
        defer cancel()
        data, _ := json.Marshal(header)
        data = append(data, 10)
        return conn.Write(ctx, websocket.MessageBinary, data)
    }
    _readWithTimeout := func () (*storage.IsdHeaderType, []byte, error) {
        ctx, cancel := context.WithTimeout(context.Background(), 33*time.Second)
        defer cancel()
        t, data, err := conn.Read(ctx)
        if err != nil {
            return nil, nil, err
        }
        if t != websocket.MessageBinary {
            return nil, nil, nil
        }
        header := &storage.IsdHeaderType{}
        i := bytes.IndexByte(data, 10)
        if i <= 0 {
            return nil, nil, fmt.Errorf("nil header")
        }
        err = json.Unmarshal(data[:i], header)
        if err != nil {
            return nil, nil, err
        }
        return header, data[i+1:], nil
    }
    headerRequest := &storage.IsdHeaderType{ Cmd: storage.IsdCmdREQUEST }
    var headerResponse *storage.IsdHeaderType
    for i := 0; i < 10; i++ {
        err = _writeWithTimeout(headerRequest)
        if err != nil {
            return err
        }
        headerResponse, _, err = _readWithTimeout()
        if err != nil {
            return err
        }
        if headerResponse == nil {
            return fmt.Errorf("nil response")
        }
        if headerResponse.Err != "" {
            slog.Info("explorer.runISD requesting.", "err", headerResponse.Err)
            time.Sleep(6789*time.Millisecond)
            continue
        }
        headerRequest.Sn = headerResponse.Sn
        headerRequest.DaaScore = headerResponse.DaaScore
        break
    }
    if headerRequest.Sn == 0 {
        return fmt.Errorf("requests exceeded")
    }
    slog.Info("explorer.runISD requested.", "sn", headerRequest.Sn, "daaScore", headerRequest.DaaScore)
    lenTotal := 0
    nilCount := 0
    headerRequest.Cmd = storage.IsdCmdPULLDAT
    if eRuntime.cfg.FullISD {
        headerRequest.Cmd = storage.IsdCmdPULLALL
    }
    var data []byte
    mhState := muhash.NewMuHash()
    keyPrefixStats := []byte(storage.KeyPrefixStateStats)
    lenPrefix := len(keyPrefixStats)
    mutex := new(sync.Mutex)
    for {
        err = _writeWithTimeout(headerRequest)
        if err != nil {
            return err
        }
        headerResponse, data, err = _readWithTimeout()
        if err != nil {
            return err
        }
        if headerResponse.Err != "" {
            return fmt.Errorf(headerResponse.Err)
        }
        if headerResponse.Done {
            break
        }
        lenData := len(data)
        if lenData == 0 {
            nilCount ++
            if nilCount > 3 {
                return fmt.Errorf("nil data")
            }
            continue
        }
        lenTotal += lenData
        wg := &sync.WaitGroup{}
        cf := -1
        for {
            i := bytes.IndexByte(data, 10)
            if i <= 0 {
                return fmt.Errorf("data invalid")
            }
            var row *storage.DataKvRowType
            row, err = storage.ParseDataKvRow(data[:i])
            if row != nil {
                if cf == -1 {
                    cf = storage.CheckKeyPrefixCF(string(row.Key))
                    if cf == 0 {
                        fmt.Print(".")
                    } else {
                        fmt.Print(",")
                    }
                }
                wg.Add(1)
                go func(row *storage.DataKvRowType) {
                    err := storage.SaveDataRowISD(cf, row)
                    if err != nil {
                        slog.Warn("storage.SaveDataRowISD failed.", "err", err)
                    } else if cf == 0 && bytes.Compare(row.Key[:lenPrefix],keyPrefixStats) != 0 {
                        mutex.Lock()
                        mhState.Add(*row.P)
                        mutex.Unlock()
                    }
                    wg.Done()
                }(row)
            }
            if i+1 >= len(data) {
                break
            }
            data = data[i+1:]
        }
        wg.Wait()
    }
    fmt.Println("")
    mhSerialized := mhState.Serialize()
    stCommitment := fmt.Sprintf("%0384x", (*mhSerialized)[:])
    slog.Info("explorer.runISD pulled.", "len", lenTotal, "stCommitment", stCommitment)
    _, rollbackList, err := storage.GetRuntimeRollbackLast(1, nil)
    if err != nil {
        return err
    }
    if len(rollbackList) != 1 || stCommitment != rollbackList[0].StCommitmentAfter {
        return fmt.Errorf("state mismatch")
    }
    slog.Info("storage.RebuildIndexTokenRocks processing.")
    err = storage.RebuildIndexTokenRocks()
    if err != nil {
        return err
    }
    return nil
}

////////////////////////////////
func dialISD(seedISD string) (*websocket.Conn, error) {
    seedISD = strings.TrimSpace(seedISD)
    if len(seedISD) < 4 {
        return nil, fmt.Errorf("seedISD invalid")
    }
    useSeed := false
    if strings.ToLower(seedISD[:4]) == "http" {
        useSeed = true
    }
    var err error
    var conn *websocket.Conn
    optSSL := &websocket.DialOptions{
        HTTPClient: &http.Client{
            Transport: &http.Transport{
                TLSClientConfig: &tls.Config{ InsecureSkipVerify: true },
            },
        },
    }
    for i := 0; i < 10; i++ {
        time.Sleep(789*time.Millisecond)
        err = nil
        conn = nil
        url := seedISD
        if useSeed {
            url, err = searchNodeISD(seedISD)
        }
        if err != nil || len(url) < 3 {
            slog.Warn("explorer.searchNodeISD failed.", "err", err)
            continue
        }
        ctx, cancel := context.WithTimeout(context.Background(), 13*time.Second)
        if strings.ToLower(url[:3]) == "wss" {
            conn, _, err = websocket.Dial(ctx, url, optSSL)
        } else {
            conn, _, err = websocket.Dial(ctx, url, nil)
        }
        cancel()
        if err != nil {
            slog.Warn("explorer.dialISD failed.", "err", err)
            continue
        }
        slog.Info("explorer.dialISD", "url", url)
        break
    }
    if conn == nil {
        return nil, fmt.Errorf("retries exceeded")
    }
    conn.SetReadLimit(10485760)
    return conn, nil
}

////////////////////////////////
func searchNodeISD(seedISD string) (string, error) {
    client := &http.Client{ Timeout: 13*time.Second }
    r, err := client.Get(seedISD)
    if err != nil {
        return "", err
    }
    defer r.Body.Close()
    if r.StatusCode != http.StatusOK {
        return "", fmt.Errorf("request failed %d", r.StatusCode)
    }
    data, err := io.ReadAll(io.LimitReader(r.Body, 2048))
    if err != nil {
        return "", err
    }
    return strings.TrimSpace(string(data)), nil
}

////////////////////////////////
func cleanISD(reopen bool) {
    slog.Info("storage.RemoveAllDataRocks")
    storage.RemoveAllDataRocks(reopen)
}
