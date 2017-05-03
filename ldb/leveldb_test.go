package ldb

import (
	"dan/pimco"
	"dan/pimco/conf"
	"dan/pimco/model"
	"fmt"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"os"
	"path"
	"testing"
	"time"
)

type dbHarness struct {
	cfg      *conf.LeveldbConfig
	db       *DB
	t        *testing.T
	flushCh  chan int64
	tagIndex *TagIndex
}

func (hs *dbHarness) Close() {
	hs.tagIndex.Close()
	hs.db.Close()
	os.RemoveAll(hs.cfg.Path)
}

func defaultConfig() conf.LeveldbConfig {
	dbPath := path.Join("/tmp", fmt.Sprintf("leveldb-test-%d", rand.Intn(1000000)))
	return conf.LeveldbConfig{
		Path: dbPath,
		Batch: conf.BatchConfig{
			BatchSize:  10,
			FlushDelay: 10,
		},
	}
}

func newHarnessWithConfig(t *testing.T, cfg conf.LeveldbConfig) *dbHarness {
	hs := &dbHarness{
		cfg:      &cfg,
		t:        t,
		flushCh:  make(chan int64, 1),
		tagIndex: NewTagIndex(cfg.Path),
	}
	cfg.Batch.OnFlush = func(offset int64) {
		hs.flushCh <- offset
	}
	cfg.TagIndex = hs.tagIndex
	hs.db = Open(cfg, 0)
	return hs
}

func newHarness(t *testing.T) *dbHarness {
	return newHarnessWithConfig(t, defaultConfig())
}

func (hs *dbHarness) reopenDB() {
	hs.db.Close()
	hs.db = Open(*hs.cfg, 0)
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
	return <-hs.flushCh
}

// check correct selection of last sample in the range
// also with unordered sample writes
func TestLDB_ReportBasic(t *testing.T) {
	/*
	   |400                439|440             479|480             520|
	   |      range 10        |     range 11      |    range 12       |
	   |   430:s1  432:s3     |                   |  490:s4  500:s2   |
	*/
	hs := newHarness(t)
	defer hs.Close()
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
}

// check that samples of other tags don't affect report
func TestLDB_ReportMultiTag(t *testing.T) {
	/*
		|400                499|500              599|
		|     range 4          |        range 5     |
		|  420:s1 440:s2       |    510:s3  530:s4  |
	*/
	hs := newHarness(t)
	defer hs.Close()
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
}

// data remain after database re-opens
func TestLDB_ReportReopenDB(t *testing.T) {
	/*
		|400                499|500              599|
		|     range 4          |        range 5     |
		|  420:s1              |    510:s2          |
	*/
	hs := newHarness(t)
	defer hs.Close()
	start := time.Now()
	step := 100
	tag := hs.makeTag()
	s1 := hs.withSample(tag, start, 0, 420)
	s2 := hs.withSample(tag, start, 0, 510)
	hs.reopenDB()
	rows := hs.runReport(tag, start, step)
	hs.expectResultRows(rows, s1, 4)
	hs.expectResultRows(rows, s2, 5)
}

// check that OnFlush return correct offset for the batch
func TestLDB_Offset(t *testing.T) {
	hs := newHarness(t)
	defer hs.Close()
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

}
