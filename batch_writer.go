package pimco

import (
	"dan/pimco/conf"
	"dan/pimco/model"
	"dan/pimco/prom"
	"log"
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

type BatchWriter struct {
	out        Writer
	ch         chan trackedSample
	wg         sync.WaitGroup
	batchSize  int
	flushDelay time.Duration
	verbose    bool
	onFlush    func(int64)
}

func NewWriter(out Writer, cfg conf.BatchConfig) *BatchWriter {
	w := BatchWriter{
		out:        out,
		ch:         make(chan trackedSample, 1000),
		batchSize:  cfg.BatchSize,
		flushDelay: time.Duration(cfg.FlushDelay) * time.Millisecond,
		onFlush:    cfg.OnFlush,
	}
	go w.writeLoop()
	return &w
}

func (w *BatchWriter) SetVerbose(value bool) {
	w.verbose = value
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
		if w.onFlush != nil {
			w.onFlush(offset)
		}
		finish := time.Now()
		for _, t := range times {
			prom.SampleWrite(finish.Sub(t))
		}
		times = times[:0]
		if w.verbose {
			log.Printf("Written batch of size %d", cnt)
		}
	}
	w.wg.Done()
}
