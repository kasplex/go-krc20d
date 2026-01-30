////////////////////////////////
package api

import (
    "os"
    "time"
    "sync"
    "strconv"
    "log/slog"
    "sync/atomic"
    jsoniter "github.com/json-iterator/go"
    "github.com/gofiber/fiber/v2"
    "github.com/gofiber/fiber/v2/middleware/limiter"
    "github.com/gofiber/fiber/v2/middleware/timeout"
    "github.com/gofiber/fiber/v2/middleware/recover"
    "github.com/gofiber/websocket/v2"
    "kasplex-executor/config"
)

////////////////////////////////
const (
    v1msgSynced = "synced"
    v1msgUnsynced = "unsynced"
    v1msgSuccessful = "successful"
    v1msgInternalError = "internal error"
    v1msgDataExpired = "data expired"
    v1msgNotReached = "not reached"
)

////////////////////////////////
var json = jsoniter.ConfigCompatibleWithStandardLibrary

////////////////////////////////
type cacheStateType struct {
    sync.RWMutex
    mtsUpdate int64
}

type runtimeType struct {
    sync.Mutex
    cfg config.ApiConfig
    serverHTTP *fiber.App
    serverWS *fiber.App
    testnet bool
}
var aRuntime runtimeType

////////////////////////////////
const bufferSizeNew = 4194304
const bufferSizeMax = 8388608

////////////////////////////////
var wsConns int32
var bufferPool = sync.Pool{
	New: func() any {
        p := new([]byte)
        *p = make([]byte, 0, bufferSizeNew)
        return p
	},
}

////////////////////////////////
func getBuffer() (*[]byte) {
    p := bufferPool.Get().(*[]byte)
    *p = (*p)[:0]
    return p
}

////////////////////////////////
func putBuffer(p *[]byte) {
	if cap(*p) <= bufferSizeMax {
        bufferPool.Put(p)
	}
}

////////////////////////////////
func Init(c chan os.Signal, cfg config.ApiConfig, testnet bool, debug int) {
    aRuntime.cfg = cfg
    aRuntime.testnet = testnet
    slog.Info("api server starting.", "host", aRuntime.cfg.Host, "port", aRuntime.cfg.Port)
    aRuntime.serverHTTP = fiber.New(fiber.Config{DisableStartupMessage:true})
    aRuntime.serverHTTP.Use(limiter.New(limiter.Config{ Max: aRuntime.cfg.ConnMax }))
    aRuntime.serverHTTP.Use(timeout.NewWithContext(func(c *fiber.Ctx) error { return c.Next() }, time.Duration(aRuntime.cfg.Timeout)*time.Second))
    aRuntime.serverHTTP.Use(recover.New())
    aRuntime.serverHTTP.Use(func(c *fiber.Ctx) error {
        c.Set("Access-Control-Allow-Origin", "*")
        c.Set("Access-Control-Allow-Methods", "GET")
        c.Set("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept")
        c.Set("Access-Control-Max-Age", "1")
        c.Set("X-Content-Type-Options", "nosniff")
        if c.Method() != "GET" {
            return c.SendStatus(403)
        }
        available, _, _, err := getInfoKRC20()
        if !available || err != nil {
            return c.SendStatus(500)
        }
        return c.Next()
    })
    aRuntime.serverHTTP.Get("/v1/info", v1routeInfo)
    aRuntime.serverHTTP.Get("/v1/krc20/op/:id", v1routeOpInfo)
    aRuntime.serverHTTP.Get("/v1/krc20/oplist", v1routeOpList)
    aRuntime.serverHTTP.Get("/v1/krc20/token/:tick", v1routeTokenInfo)
    aRuntime.serverHTTP.Get("/v1/krc20/tokenlist", v1routeTokenList)
    aRuntime.serverHTTP.Get("/v1/krc20/address/:address/token/:tick", v1routeAddressTokenInfo)
    aRuntime.serverHTTP.Get("/v1/krc20/address/:address/tokenlist", v1routeAddressTokenList)
    aRuntime.serverHTTP.Get("/v1/krc20/market/:tick", v1routeMarketList)
    aRuntime.serverHTTP.Get("/v1/krc20/blacklist/:tick", v1routeBlackList)
    aRuntime.serverHTTP.Get("/v1/archive/oplist/:oprange", v1ArchiveOpList)
    aRuntime.serverHTTP.Get("/v1/debug/database/:cf", v1DebugDatabaseSeek)
    aRuntime.serverHTTP.All("*", func(c *fiber.Ctx) (error) {
        return c.SendStatus(404)
    })
    go func() {
        err := aRuntime.serverHTTP.Listen(aRuntime.cfg.Host + ":" + strconv.Itoa(aRuntime.cfg.Port))
        if err != nil {
            slog.Warn("api server down.", "error", err.Error())
        } else {
            slog.Info("api server down.")
        }
        c <- os.Interrupt
    }()
    InitSync(c)
    time.Sleep(345 * time.Millisecond)
}

////////////////////////////////
func InitSync(c chan os.Signal) {
    if aRuntime.cfg.PortSync <= 0 || aRuntime.cfg.PortSync == aRuntime.cfg.Port || aRuntime.cfg.SyncMax <= 0 {
        return
    }
    slog.Info("sync server starting.", "host", aRuntime.cfg.Host, "port", aRuntime.cfg.PortSync)
    aRuntime.serverWS = fiber.New(fiber.Config{DisableStartupMessage:true})
    aRuntime.serverWS.Get("/", func(c *fiber.Ctx) error {
		if !websocket.IsWebSocketUpgrade(c) {
			return fiber.ErrUpgradeRequired
		}
        n := atomic.LoadInt32(&wsConns)
        if n >= aRuntime.cfg.SyncMax {
            return c.SendStatus(429)
        }
		return c.Next()
	}, websocket.New(func(conn *websocket.Conn) {
        n := atomic.AddInt32(&wsConns, 1)
		defer func() {
			atomic.AddInt32(&wsConns, -1)
			conn.Close()
		}()
        if n > aRuntime.cfg.SyncMax {
            return
        }
        v1syncISD(conn)
	}))
    go func() {
        err := aRuntime.serverWS.Listen(aRuntime.cfg.Host + ":" + strconv.Itoa(aRuntime.cfg.PortSync))
        if err != nil {
            slog.Warn("sync server down.", "error", err.Error())
        } else {
            slog.Info("sync server down.")
        }
        c <- os.Interrupt
    }()
}

////////////////////////////////
func Shutdown() {
    if aRuntime.serverHTTP != nil {
        aRuntime.serverHTTP.Shutdown()
    }
    if aRuntime.serverWS != nil {
        aRuntime.serverWS.Shutdown()
    }
}
