package rpc

import (
	"fmt"
	"math/big"
)

var externalCurrency = map[string]int{
	"cny": 1,
	"krw": 1,
}

func fl(val string) (out *big.Float, err error) {
	value, ret := new(big.Float).SetString(val)
	if !ret {
		err = fmt.Errorf("invalid va")
		return
	}
	return value, err
}

// returns x / y
func quo(x, y *big.Float) *big.Float {
	return new(big.Float).Quo(x, y)
}

// returns x * y
func mul(x, y *big.Float) *big.Float {
	return new(big.Float).Mul(x, y)
}
