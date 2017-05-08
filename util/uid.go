package util

import (
	"encoding/hex"
	"math/rand"
)

func NewUID() string {
	var buf [16]byte
	rand.Read(buf[:])
	return hex.EncodeToString(buf[:])
}
