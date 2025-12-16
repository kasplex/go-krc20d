
////////////////////////////////
package misc

import (
    "github.com/kaspanet/go-muhash"
)

////////////////////////////////
type MuHashQueueType struct {
    Remove bool
    Data []byte
}

////////////////////////////////
func MuHashNew(capQueue int) ([]MuHashQueueType) {
    queue := make([]MuHashQueueType, 0, capQueue)
    return queue
}

////////////////////////////////
func MuHashAdd(queue []MuHashQueueType, data []byte) ([]MuHashQueueType) {
    queue = append(queue, MuHashQueueType{
        Remove: false,
        Data: data,
    })
    return queue
}

////////////////////////////////
func MuHashRemove(queue []MuHashQueueType, data []byte) ([]MuHashQueueType) {
    queue = append(queue, MuHashQueueType{
        Remove: true,
        Data: data,
    })
    return queue
}

////////////////////////////////
func MuHashSerialize(mhBase []byte, queue []MuHashQueueType, batch bool) ([]byte, error) {
    var err error
    var mh *muhash.MuHash
    if len(mhBase) > 0 {
        var mhSerialized muhash.SerializedMuHash
        copy(mhSerialized[:], mhBase)
        mh, err = muhash.DeserializeMuHash(&mhSerialized)
        if err != nil {
            return nil, err
        }
    } else {
        mh = muhash.NewMuHash()
    }
    if batch {
        lenQueue := len(queue)
        var mhList [nGoroutine]*muhash.MuHash
        for i := range mhList {
            mhList[i] = muhash.NewMuHash()
        }
        GoBatch(lenQueue, func(i int, iBatch int) (error) {
            if queue[i].Remove {
                mhList[iBatch].Remove(queue[i].Data)
            } else {
                mhList[iBatch].Add(queue[i].Data)
            }
            return nil
        })
        for i := range mhList {
            mh.Combine(mhList[i])
        }
    } else {
        for i := range queue {
            if queue[i].Remove {
                mh.Remove(queue[i].Data)
            } else {
                mh.Add(queue[i].Data)
            }
        }
    }
    mhSerialized := mh.Serialize()
    return (*mhSerialized)[:], nil
}
