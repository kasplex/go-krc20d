
////////////////////////////////
package misc

import (
    "time"
    "golang.org/x/sync/errgroup"
)

////////////////////////////////
const nGoroutine = 8

////////////////////////////////
func GoBatch(lenBatch int, fGo func(int,int) (error)) (int64, error) {
    if lenBatch <= 0 {
        return 0, nil
    }
    mtss := time.Now().UnixMilli()
    nBatch := (lenBatch + nGoroutine - 1) / nGoroutine
    g := &errgroup.Group{}
    for i := 0; i < nGoroutine; i ++ {
        index := i
        g.Go(func() error {
            for j := index*nBatch; j < (index+1)*nBatch; j ++ {
                if j >= lenBatch {
                    break
                }
                err := fGo(j, index)
                if err != nil {
                    return err
                }
            }
            return nil
        })
    }
    err := g.Wait()
    if err != nil {
        return 0, err
    }
    return time.Now().UnixMilli() - mtss, nil
}
