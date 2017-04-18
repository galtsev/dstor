package util

import (
	"time"
)

type DurationHeap []time.Duration

func (h DurationHeap) Len() int           { return len(h) }
func (h DurationHeap) Less(i, j int) bool { return h[i] < h[j] }
func (h DurationHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *DurationHeap) Push(x interface{}) {
	*h = append(*h, x.(time.Duration))
}

func (h *DurationHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
