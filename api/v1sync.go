////////////////////////////////
package api

import (
    "time"
    "github.com/gofiber/websocket/v2"
    "kasplex-executor/storage"
)

////////////////////////////////
func v1syncISD(conn *websocket.Conn) {
    headerResponse := storage.IsdHeaderType{ Cmd: storage.IsdCmdRESPONS }
    pDataResponse := getBuffer()
    defer putBuffer(pDataResponse)
    _applyheaderResponse := func () {
        headerJSON, _ := json.Marshal(&headerResponse)
        *pDataResponse = (*pDataResponse)[:0]
        *pDataResponse = append(*pDataResponse, headerJSON...)
        *pDataResponse = append(*pDataResponse, 10)
    }
	for {
        conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		t, dataRequest, err := conn.ReadMessage()
		if err != nil {
			break
		}
        if t != websocket.BinaryMessage || len(dataRequest) > 256 {
			continue
		}
        headerRequest := storage.IsdHeaderType{}
        err = json.Unmarshal(dataRequest, &headerRequest)
        if err != nil {
			continue
        }
        headerResponse.Err = ""
        done := false
        switch headerRequest.Cmd {
        case storage.IsdCmdREQUEST:
            if headerResponse.Sn > 0 {
                break
            }
            sn, daaScore, err := storage.RequestISD()
            if err != nil {
                headerResponse.Err = err.Error()
            } else {
                headerResponse.Sn = sn
                headerResponse.DaaScore = daaScore
            }
            _applyheaderResponse()
        case storage.IsdCmdPULLDAT:
            if headerRequest.Sn != headerResponse.Sn {
                headerResponse.Err = "snapshot expired"
            }
            _applyheaderResponse()
            if headerResponse.Err != "" || headerResponse.Done {
                done = true
                break
            }
            cf, key, err := storage.SeekDataISD(headerResponse.Cf, headerResponse.Key, pDataResponse, bufferSizeNew)
            if err != nil {
                headerResponse.Err = "seek failed"
                _applyheaderResponse()
                done = true
                break
            } else {
                headerResponse.Cf = cf
                headerResponse.Key = key
            }
            if len(key) == 0 {
                headerResponse.Done = true
            }
        }
        if len(*pDataResponse) > 0 {
            conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
            err = conn.WriteMessage(websocket.BinaryMessage, *pDataResponse)
            if err != nil {
                break
            }
        }
        if done {
            break
        }
	}
    if headerResponse.Sn > 0 {
        storage.DisconnectISD()
    }
}
