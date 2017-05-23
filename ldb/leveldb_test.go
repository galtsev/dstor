package ldb

import (
	"fmt"
	"github.com/galtsev/dstor"
	"github.com/galtsev/dstor/conf"
	"github.com/galtsev/dstor/model"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"os"
	"path"
	"testing"
	"time"
)

type dbHarness struct {
	cfg      conf.LeveldbConfig
	openFn   func() pimco.Backend
	db       pimco.Backend
	t        *testing.T
	flushCh  chan int64
	tagIndex *TagIndex
}

func (hs *dbHarness) Close() {
	if hs.tagIndex != nil {
		hs.tagIndex.Close()
	}
	hs.db.Close()
	os.RemoveAll(hs.cfg.Path)
}

func new(t *testing.T) *dbHarness {
	cfg := conf.LeveldbConfig{
		Path: path.Join("/tmp", fmt.Sprintf("leveldb-test-%d", rand.Intn(1000000))),
		Batch: conf.BatchConfig{
			BatchSize:  10,
			FlushDelay: 10,
		},
		NumPartitions: 1,
		Partitions:    []int32{0},
	}
	hs := &dbHarness{
		cfg:     cfg,
		t:       t,
		flushCh: make(chan int64, 2),
	}
	return hs
}

type testContext struct {
	*TagIndex
	ch chan int64
}

func (ctx testContext) OnFlush(offset int64) {
	ctx.ch <- offset
}

func newHarness(t *testing.T) *dbHarness {
	hs := new(t)
	hs.tagIndex = NewTagIndex(hs.cfg.Path)
	hs.openFn = func() pimco.Backend {
		ctx := testContext{
			TagIndex: hs.tagIndex,
			ch:       hs.flushCh,
		}
		return Open(hs.cfg, int32(0), ctx)
	}
	hs.db = hs.openFn()
	return hs
}

type clusterCtx struct {
	ch chan int64
}

func (ctx clusterCtx) OnFlush(partition int32, offset int64) {
	ctx.ch <- offset
}

func newCluster(t *testing.T) *dbHarness {
	hs := new(t)
	hs.cfg.NumPartitions = 2
	hs.cfg.Partitions = []int32{0, 1}
	hs.openFn = func() pimco.Backend {
		return NewCluster(hs.cfg, clusterCtx{hs.flushCh})
	}
	hs.db = hs.openFn()
	return hs
}

func withHarness(t *testing.T, fn func(*dbHarness)) {
	for _, factory := range []func(t *testing.T) *dbHarness{newHarness, newCluster} {
		//for _, factory := range []func(t *testing.T) *dbHarness{newHarness} {
		hs := factory(t)
		defer hs.Close()
		fn(hs)
	}
}

func (hs *dbHarness) reopenDB() {
	hs.db.Close()
	hs.db = hs.openFn()
}

func (hs *dbHarness) runReport(tag string, start time.Time, step int) []pimco.ReportLine {
	return hs.db.Report(tag, start, start.Add(time.Duration(step*100)*time.Second))
}

func (hs *dbHarness) expectResultRows(rows []pimco.ReportLine, expected *model.Sample, actualIndex ...int) {
	for _, idx := range actualIndex {
		row := rows[idx]
		for vid := range expected.Values {
			assert.Equal(hs.t, expected.Values[vid], row.Values[vid])
		}
	}
}

func (hs *dbHarness) makeTag() string {
	return fmt.Sprintf("newtag-%d", rand.Intn(100000))
}

/* ts - sample time, seconds since start */
func (hs *dbHarness) withSample(tag string, start time.Time, offset int64, ts int) *model.Sample {
	sample := model.Sample{
		Tag: tag,
		TS:  start.Add(time.Duration(ts) * time.Second).UnixNano(),
	}
	for i := range sample.Values {
		sample.Values[i] = rand.Float64()
	}
	hs.db.AddSample(&sample, offset)
	return &sample
}

func (hs *dbHarness) flush() int64 {
	select {
	case offset := <-hs.flushCh:
		return offset
	case <-time.After(time.Second):
		hs.t.Error("Timed out")
	}
	return 0
}

// check correct selection of last sample in the range
// also with unordered sample writes
func TestLDB_ReportBasic(t *testing.T) {
	/*
	   |400                439|440             479|480             520|
	   |      range 10        |     range 11      |    range 12       |
	   |   430:s1  432:s3     |                   |  490:s4  500:s2   |
	*/
	withHarness(t, func(hs *dbHarness) {
		start := time.Now()
		step := 40 //seconds
		t1 := hs.makeTag()
		// put one sample (s1) to range 10 (start+400s..start+440s)
		s1 := hs.withSample(t1, start, 0, 430)
		hs.flush()
		rows := hs.runReport(t1, start, step)
		hs.expectResultRows(rows, s1, 10, 11, 20, 21)
		// put one more sample to range 12 (start+480s..start+520s)
		s2 := hs.withSample(t1, start, 0, 500)
		hs.flush()
		rows = hs.runReport(t1, start, step)
		hs.expectResultRows(rows, s1, 10, 11)
		hs.expectResultRows(rows, s2, 12, 13, 20)
		// s3 in range 10, override s1, but not s2
		s3 := hs.withSample(t1, start, 0, 432)
		hs.flush()
		rows = hs.runReport(t1, start, step)
		hs.expectResultRows(rows, s3, 10, 11)
		hs.expectResultRows(rows, s2, 12, 14, 30)
		// s4 in range 12, but before s2 will not override s2
		_ = hs.withSample(t1, start, 0, 490)
		hs.flush()
		rows = hs.runReport(t1, start, step)
		hs.expectResultRows(rows, s2, 12, 14, 30)
	})
}

// check that samples of other tags don't affect report
func TestLDB_ReportMultiTag(t *testing.T) {
	/*
		|400                499|500              599|
		|     range 4          |        range 5     |
		|  420:s1 440:s2       |    510:s3  530:s4  |
	*/
	withHarness(t, func(hs *dbHarness) {
		start := time.Now()
		step := 100
		t1 := hs.makeTag()
		t2 := hs.makeTag()
		s1 := hs.withSample(t1, start, 0, 420)
		s2 := hs.withSample(t2, start, 0, 440)
		s3 := hs.withSample(t2, start, 0, 510)
		s4 := hs.withSample(t1, start, 0, 530)
		hs.flush()
		rows := hs.runReport(t1, start, step)
		hs.expectResultRows(rows, s1, 4)
		hs.expectResultRows(rows, s4, 5)
		rows = hs.runReport(t2, start, step)
		hs.expectResultRows(rows, s2, 4)
		hs.expectResultRows(rows, s3, 5)
	})
}

// data remain after database re-opens
func TestLDB_ReportReopenDB(t *testing.T) {
	/*
		|400                499|500              599|
		|     range 4          |        range 5     |
		|  420:s1              |    510:s2          |
	*/
	withHarness(t, func(hs *dbHarness) {
		start := time.Now()
		step := 100
		tag := hs.makeTag()
		s1 := hs.withSample(tag, start, 0, 420)
		s2 := hs.withSample(tag, start, 0, 510)
		hs.reopenDB()
		rows := hs.runReport(tag, start, step)
		hs.expectResultRows(rows, s1, 4)
		hs.expectResultRows(rows, s2, 5)
	})
}

// check that OnFlush return correct offset for the batch
func TestLDB_Offset(t *testing.T) {
	withHarness(t, func(hs *dbHarness) {
		start := time.Now()
		tag := hs.makeTag()
		// case 1 - offsets ordered on write
		hs.withSample(tag, start, 0, 420)
		hs.withSample(tag, start, 1, 510)
		assert.Equal(t, int64(1), hs.flush())

		// case 2 - offsets in reverse order
		hs.withSample(tag, start, 3, 520)
		hs.withSample(tag, start, 2, 530)
		assert.Equal(t, int64(3), hs.flush())
	})
}
