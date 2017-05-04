package pimco

import (
	"dan/pimco/conf"
	"dan/pimco/model"
	"dan/pimco/prom"
	"sync"
	"time"
)

type Writer interface {
	Add(sample *model.Sample)
	Flush()
	Close()
}

type trackedSample struct {
	sample model.Sample
	start  time.Time
	offset int64
}

type BatchContext interface {
	OnFlush(offset int64)
}

type FakeContext struct{}

func (ctx FakeContext) OnFlush(offset int64) {}

type BatchWriter struct {
	out        Writer
	ch         chan trackedSample
	wg         sync.WaitGroup
	batchSize  int
	flushDelay time.Duration
	ctx        BatchContext
}

func NewWriter(out Writer, cfg conf.BatchConfig, ctx BatchContext) *BatchWriter {
	w := BatchWriter{
		out:        out,
		ch:         make(chan trackedSample, 1000),
		batchSize:  cfg.BatchSize,
		flushDelay: time.Duration(cfg.FlushDelay) * time.Millisecond,
		ctx:        ctx,
	}
	go w.writeLoop()
	return &w
}

func (w *BatchWriter) Write(sample *model.Sample, offset int64) {
	ts := trackedSample{
		sample: *sample,
		start:  time.Now(),
		offset: offset,
	}
	w.ch <- ts
}

func (w *BatchWriter) Close() {
	w.wg.Add(1)
	close(w.ch)
	w.wg.Wait()
}

func (w *BatchWriter) writeLoop() {
	var chanClosed bool
	timer := time.NewTimer(w.flushDelay)
	// store batched samples arrival time here for delay tracking
	var times []time.Time
	for !chanClosed {
		var offset int64 = 0
		var cnt int = 0
		// reset to initial state (empty batch)
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}

		// wait for first sample or channel close, no timeout
		sample, ok := <-w.ch
		if !ok {
			// channel closed, no samples collected yet, just exit
			break
		}
		w.out.Add(&sample.sample)
		times = append(times, sample.start)
		offset = sample.offset
		cnt++
		timer.Reset(w.flushDelay)
		flush := false
		for !flush {
			select {
			case sample, ok := <-w.ch:
				if ok {
					w.out.Add(&sample.sample)
					times = append(times, sample.start)
					if sample.offset > offset {
						offset = sample.offset
					}
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
		if w.ctx != nil {
			w.ctx.OnFlush(offset)
		}
		finish := time.Now()
		for _, t := range times {
			prom.SampleWrite(finish.Sub(t))
		}
		times = times[:0]
	}
	w.wg.Done()
}
