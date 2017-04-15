package pimco

import (
	"dan/pimco/model"
	"log"
	"sync"
	"time"
)

type Writer interface {
	Add(sample *model.Sample)
	Flush()
	Close()
}

type BatchWriter struct {
	out        Writer
	ch         chan model.Sample
	wg         sync.WaitGroup
	batchSize  int
	flushDelay time.Duration
	verbose    bool
}

func NewWriter(out Writer, batchSize int, flushDelay time.Duration) *BatchWriter {
	w := BatchWriter{
		out:        out,
		ch:         make(chan model.Sample, 1000),
		batchSize:  batchSize,
		flushDelay: flushDelay,
	}
	go w.writeLoop()
	return &w
}

func (w *BatchWriter) SetVerbose(value bool) {
	w.verbose = value
}

func (w *BatchWriter) Write(sample *model.Sample) {
	w.ch <- *sample
}

func (w *BatchWriter) Close() {
	w.wg.Add(1)
	close(w.ch)
	w.wg.Wait()
}

func (w *BatchWriter) writeLoop() {
	var cnt int
	var chanClosed bool
	timer := time.NewTimer(w.flushDelay)
	for !chanClosed {
		// reset to initial state (empty batch)
		w.out.Flush()
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		cnt = 0

		// wait for first sample or channel close, no timeout
		sample, ok := <-w.ch
		if !ok {
			// channel closed, no samples collected yet, just exit
			break
		}
		w.out.Add(&sample)
		cnt++
		timer.Reset(w.flushDelay)
		flush := false
		for !flush {
			select {
			case sample, ok := <-w.ch:
				if ok {
					w.out.Add(&sample)
					cnt++
					if cnt >= w.batchSize {
						flush = true
					}
				} else {
					flush = true
					chanClosed = true
				}
			case <-timer.C:
				flush = true
			}
		}
		w.out.Flush()
		if w.verbose {
			log.Printf("Writing batch of size %d", cnt)
		}
	}
	w.wg.Done()
}
