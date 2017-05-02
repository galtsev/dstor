package ldb

import (
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
	dbPath  string
	db      *DB
	t       *testing.T
	flushCh chan int64
}

func (hs *dbHarness) Close() {
	if hs.timeout != nil {
		hs.timeout.Stop()
	}
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
		dbPath: cfg.Path,
		t:      t,
	}
	cfg.Batch.OnFlush = func(offset int64) {
		hs.flushCh <- offset
	}
	hs.db = Open(cfg, 0)
	return hs
}

func newHarness(t *testing.T) *dbHarness {
	return newHarnessWithConfig(t, defaultConfig())
}

func (hs *dbHarness) withSamples(dest *[]model.Sample, tag string, offset int64, ts ...int64) *dbHarness {
	for _, t := range ts {
		sample := model.Sample{
			Tag: tag,
			TS:  t,
		}
		for i := range sample.Values {
			sample.Values[i] = rand.Float64()
		}
		hs.db.AddSample(&sample, offset)
		*dest = append(*dest, sample)
	}
	return hs
}

func (hs *dbHarness) flush() int64 {
	return <-hs.flushCh
}

func TestLDB_Simple(t *testing.T) {
	hs := newHarness(t)
	defer hs.Close()
	tag := "tag1"
	reportStart := time.Now()
	reportEnd := reportStart.Add(time.Duration(40*100) * time.Second)
	var samples []model.Sample
	hs.withSamples(&samples, tag, 0, reportStart.Add(time.Duration(30)*time.Second).UnixNano())
	hs.flush()
	res := hs.db.Report(tag, reportStart, reportEnd)
	assert.Equal(hs.t, samples[0].Values[0], res[0].Values[0])
}
