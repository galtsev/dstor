package pimco

import (
	"dan/pimco/conf"
	"dan/pimco/model"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

type testWriter struct {
	t       *testing.T
	writer  *BatchWriter
	batch   []*model.Sample
	samples []*model.Sample
	flushCh chan int64
}

func newTestWriter(t *testing.T) *testWriter {
	cfg := conf.BatchConfig{
		BatchSize:  10,
		FlushDelay: 10,
	}
	w := &testWriter{
		t:       t,
		flushCh: make(chan int64, 4),
	}
	w.writer = NewWriter(w, cfg, w)
	return w
}

func (w *testWriter) flush() int64 {
	select {
	case res := <-w.flushCh:
		return res
	case <-time.After(time.Second):
		w.t.Error("Timed out")
	}
	return 0
}

func (w *testWriter) withSamples(dest *[]model.Sample, tag string, offset int64, ts ...int64) {
	for _, t := range ts {
		sample := model.Sample{
			Tag: tag,
			TS:  t,
		}
		w.writer.Write(&sample, offset)
		*dest = append(*dest, sample)
	}
}

func (w *testWriter) assertSamples(expect []model.Sample) {
	assert.Equal(w.t, len(expect), len(w.samples))
	for i := range expect {
		assert.Equal(w.t, expect[i].TS, w.samples[i].TS)
	}
}

func (w *testWriter) OnFlush(offset int64) {
	w.flushCh <- offset
}

func (w *testWriter) Add(sample *model.Sample) {
	w.batch = append(w.batch, sample)
}

func (w *testWriter) Flush() {
	w.samples = append(w.samples, w.batch...)
	w.batch = w.batch[:0]
}

func (w *testWriter) Close() {

}

func TestBatch_Simple(t *testing.T) {
	tw := newTestWriter(t)
	var samples []model.Sample
	tw.withSamples(&samples, "one", 1, 1)
	tw.flush()
	tw.assertSamples(samples)
}
