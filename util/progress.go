package util

import (
	"fmt"
	"time"
)

type Progress struct {
	cnt int
	bs  int
	t   time.Time
}

func (p *Progress) Step() {
	p.cnt++
	if p.cnt%p.bs == 0 {
		fmt.Printf("%10d %10d\n", p.cnt/p.bs, int(time.Since(p.t))/1000000)
		p.t = time.Now()
	}
}

func NewProgress(batchSize int) *Progress {
	return &Progress{
		bs: batchSize,
		t:  time.Now(),
	}
}
