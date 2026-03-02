
////////////////////////////////
package api

import (
    "fmt"
    "strings"
    "strconv"
    "encoding/hex"
    "math/big"
    "go-krc20d/misc"
)

////////////////////////////////
var tickIgnored = map[string]bool{
    "KASPA": true, "KASPLX": true,
    "KASP": true, "WKAS": true,
    "GIGA": true, "WBTC": true,
    "WETH": true, "USDT": true,
    "USDC": true, "FDUSD": true,
    "USDD": true, "TUSD": true,
    "USDP": true, "PYUSD": true,
    "EURC": true, "BUSD": true,
    "GUSD": true, "EURT": true,
    "XAUT": true, "TETHER": true,
    // ...
}

////////////////////////////////
// Filter of the tick string for 4-6 chars.
func filterTick(tick string) (string, error) {
    tick = strings.TrimSpace(tick)
    lenTick := len(tick)
    if lenTick >= 64 {
        var err error
        tick, err = filterHash(tick)
        return tick, err
    }
    tick = strings.ToUpper(tick)
    if (lenTick < 4 || lenTick > 6) {
        return "", fmt.Errorf("invalid")
    }
    for i := 0; i < lenTick; i++ {
        if (tick[i] < 65 || tick[i] > 90) {
            return "", fmt.Errorf("invalid")
        }
    }
    if tickIgnored[tick] {
        return tick, fmt.Errorf("ignored")
    }
    return tick, nil
}

////////////////////////////////
// Filter of the address string.
func filterAddress(address string) (string, error) {
    address = strings.TrimSpace(address)
    address = strings.ToLower(address)
    if address == "" {
        return "", fmt.Errorf("invalid")
    }
    if !misc.VerifyAddr(address, aRuntime.testnet) {
        return "", fmt.Errorf("invalid")
    }
    return address, nil
}

////////////////////////////////
// Filter of the uint64 string.
func filterUint(value string) (uint64, error) {
    value = strings.TrimSpace(value)
    if value == "" {
        return 0, fmt.Errorf("invalid")
    }
    valueUint, err := strconv.ParseUint(value, 10, 64)
    if err != nil {
        return 0, err
    }
    valueString := strconv.FormatUint(valueUint, 10)
    if (valueString != value) {
        return 0, fmt.Errorf("invalid")
    }
    return valueUint, nil
}

////////////////////////////////
// Filter of the uint64 string.
func filterUintString(value string) (string, error) {
    value = strings.TrimSpace(value)
    if value == "" {
        return "", fmt.Errorf("invalid")
    }
    valueUint, _ := filterUint(value)
    valueString := strconv.FormatUint(valueUint, 10)
    if (valueString != value) {
        return "", fmt.Errorf("invalid")
    }
    return value, nil
}

////////////////////////////////
// Filter of the hash string.
func filterHash(hash string) (string, error) {
    hash = strings.TrimSpace(hash)
    hash = strings.ToLower(hash)
    if len(hash) != 64 {
        return "", fmt.Errorf("invalid")
    }
    _, err := hex.DecodeString(hash)
    if err != nil {
        return "", err
    }
    return hash, nil
}

////////////////////////////////
// Filter of the amount string.
func filterAmount(amount string) (string, error) {
    amount = strings.TrimSpace(amount)
    if amount == "" {
        return "", fmt.Errorf("invalid")
    }
    amountBig := new(big.Int)
    _, s := amountBig.SetString(amount, 10)
    if !s {
        return "", fmt.Errorf("invalid")
    }
    amount2 := amountBig.Text(10)
    if amount != amount2 {
        return "", fmt.Errorf("invalid")
    }
    limitBig := new(big.Int)
    limitBig.SetString("0", 10)
    if limitBig.Cmp(amountBig) >= 0 {
        return "", fmt.Errorf("invalid")
    }
    limitBig.SetString("99999999999999999999999999999999", 10)
    if amountBig.Cmp(limitBig) > 0 {
        return "", fmt.Errorf("invalid")
    }
    return amount, nil
}

////////////////////////////////
// Filter of the dec string.
func filterDec(dec string) (string, error) {
    dec = strings.TrimSpace(dec)
    if dec == "" {
        return "", fmt.Errorf("invalid")
    }
    decInt, err := strconv.Atoi(dec)
    if err != nil {
        return "", fmt.Errorf("invalid")
    }
    decString := strconv.Itoa(decInt)
    if (decString != dec || decInt < 0 || decInt > 18) {
        return "", fmt.Errorf("invalid")
    }
    return dec, nil
}

////////////////////////////////
// Filter of the tick and txid string.
func filterTickTxid(tick string) (string, error) {
    if len(tick) < 64 {
        return filterTick(tick)
    }
    return filterHash(tick)
}
