package pimco

import (
	"sync/atomic"
)

var latest int64

func SetLatest(ts int64) {
	atomic.StoreInt64(&latest, ts)
}

func GetLatest() int64 {
	return atomic.LoadInt64(&latest)
}
