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
	dbPath   string
	db       *DB
	t        *testing.T
	flushCh  chan int64
	tagIndex *TagIndex
	report   struct {
		tag    string
		start  time.Time
		step   int //seconds
		result []pimco.ReportLine
	}
}

func (hs *dbHarness) Close() {
	hs.tagIndex.Close()
	hs.db.Close()
	os.RemoveAll(hs.dbPath)
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
		dbPath:   cfg.Path,
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

func (hs *dbHarness) withReport(step int) *dbHarness {
	hs.report.start = time.Now()
	hs.report.step = step
	hs.report.tag = fmt.Sprintf("newtag-%d", rand.Intn(100000))
	return hs
}

func (hs *dbHarness) runReport() *dbHarness {
	hs.report.result = hs.db.Report(hs.report.tag, hs.report.start, hs.report.start.Add(time.Duration(hs.report.step*100)*time.Second))
	return hs
}

func (hs *dbHarness) expectResultRows(expected *model.Sample, actualIndex ...int) {
	for _, idx := range actualIndex {
		for vid := range expected.Values {
			assert.Equal(hs.t, expected.Values[vid], hs.report.result[idx].Values[vid])
		}
	}
}

/* ts - sample time, seconds since start */
func (hs *dbHarness) withSample(offset int64, ts int) *model.Sample {
	sample := model.Sample{
		Tag: hs.report.tag,
		TS:  hs.report.start.Add(time.Duration(ts) * time.Second).UnixNano(),
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

func TestLDB_Report(t *testing.T) {
	/*
	   |400                439|440             479|480             520|
	   |      range 10        |     range 11      |    range 12       |
	   |   430:s1  432:s3     |                   |  490:s4  500:s2   |
	*/
	hs := newHarness(t)
	defer hs.Close()
	hs.withReport(40)
	// put one sample (s1) to range 10 (start+400s..start+440s)
	s1 := hs.withSample(0, 430)
	hs.flush()
	hs.runReport()
	hs.expectResultRows(s1, 10, 11, 20, 21)
	// put one more sample to range 12 (start+480s..start+520s)
	s2 := hs.withSample(0, 500)
	hs.flush()
	hs.runReport()
	hs.expectResultRows(s1, 10, 11)
	hs.expectResultRows(s2, 12, 13, 20)
	// s3 in range 10, override s1, but not s2
	s3 := hs.withSample(0, 432)
	hs.flush()
	hs.runReport()
	hs.expectResultRows(s3, 10, 11)
	hs.expectResultRows(s2, 12, 14, 30)
	// s4 in range 12, but before s2 will not override s2
	_ = hs.withSample(0, 490)
	hs.flush()
	hs.runReport()
	hs.expectResultRows(s2, 12, 14, 30)
}
