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
		tag     string
		start   time.Time
		step    int //seconds
		result  []pimco.ReportLine
		samples []model.Sample
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

func (hs *dbHarness) expectResultValues(vindex int, expect map[int]int) {
	for rid, sid := range expect {
		assert.Equal(hs.t, hs.report.samples[sid].Values[vindex], hs.report.result[rid].Values[vindex])
	}
}

/* ts - sample time, seconds since start */
func (hs *dbHarness) withSamples(offset int64, ts ...int) *dbHarness {
	for _, t := range ts {
		sample := model.Sample{
			Tag: hs.report.tag,
			TS:  hs.report.start.Add(time.Duration(t) * time.Second).UnixNano(),
		}
		for i := range sample.Values {
			sample.Values[i] = rand.Float64()
		}
		hs.db.AddSample(&sample, offset)
		hs.report.samples = append(hs.report.samples, sample)
	}
	return hs
}

func (hs *dbHarness) flush() int64 {
	return <-hs.flushCh
}

func TestLDB_Report(t *testing.T) {
	hs := newHarness(t)
	defer hs.Close()
	hs.withReport(40).withSamples(0, 30).flush()
	hs.runReport().expectResultValues(0, map[int]int{0: 0, 1: 0, 2: 0})
}
