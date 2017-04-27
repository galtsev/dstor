package pimco

import (
	"sync/atomic"
)

var latest int64

func SetLatest(ts int64) {
	if ts > GetLatest() {
		atomic.StoreInt64(&latest, ts)
	}
}

func GetLatest() int64 {
	return atomic.LoadInt64(&latest)
}
