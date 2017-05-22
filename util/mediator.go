package util

import (
	"time"
)

type Mediator struct {
	last time.Time
	rate int
	step time.Duration
}

func NewMediator(rate int, step time.Duration) *Mediator {
	return &Mediator{
		last: time.Now(),
		rate: rate,
		step: step,
	}
}

func (m *Mediator) Next() int {
	now := time.Now()
	next := m.last.Add(m.step)
	time.Sleep(next.Sub(now))
	m.last = time.Now()
	return m.rate * int(m.step) / int(time.Second)
}
