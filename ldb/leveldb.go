package ldb

import (
	"dan/pimco"
	. "dan/pimco/base"
	"dan/pimco/model"
	"dan/pimco/serializer"
	"encoding/binary"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
	"log"
	"time"
)

var BigEndian = binary.BigEndian

type DB struct {
	db       *leveldb.DB
	szr      serializer.Serializer
	tagIndex map[string]uint32
	batch    leveldb.Batch
	writer   *pimco.BatchWriter
}

func Open(cfg pimco.LeveldbConfig, partition int32) *DB {
	opts := opt.Options{
		WriteBuffer:                   cfg.Opts.WriteBufferMb * opt.MiB,
		CompactionTableSize:           cfg.Opts.CompactionTableSizeMb * opt.MiB,
		CompactionTotalSizeMultiplier: cfg.Opts.CompactionTotalSizeMultiplier,
		WriteL0SlowdownTrigger:        cfg.Opts.WriteL0SlowdownTrigger,
		WriteL0PauseTrigger:           cfg.Opts.WriteL0PauseTrigger,
	}
	partitionPath := fmt.Sprintf("%s/%d", cfg.Path, partition)
	ldb, err := leveldb.OpenFile(partitionPath, &opts)
	Check(err)
	db := DB{
		db: ldb,
	}
	// TODO implement real map
	db.tagIndex = make(map[string]uint32)
	for i := 0; i < 20; i++ {
		tag := fmt.Sprintf("tag%d", i)
		db.tagIndex[tag] = uint32(i)
	}
	db.szr = serializer.NewSerializer("msgp")
	db.writer = pimco.NewWriter(&db, cfg.BatchSize, time.Duration(cfg.FlushDelay)*time.Millisecond)
	return &db
}

func (db *DB) Add(sample *model.Sample) {
	key := db.MakeKey(sample.Tag, sample.TS)
	body := db.szr.Marshal(sample)
	db.batch.Put(key, body)
}

func (db *DB) Flush() {
	db.db.Write(&db.batch, nil)
	db.batch.Reset()
}

func (db *DB) Close() {
	db.Flush()
	db.db.Close()
}

func (db *DB) AddSample(sample *model.Sample) {
	db.writer.Write(sample)
}

func (db *DB) MakeKey(tag string, ts int64) []byte {
	var res [12]byte
	ti, ok := db.tagIndex[tag]
	if !ok {
		log.Panicf("Undefined tag:%s", tag)
	}
	BigEndian.PutUint32(res[:4], ti)
	BigEndian.PutUint64(res[4:], uint64(ts))
	return res[:]
}

func (db *DB) ReportOne(tag string, ts int64) (*model.Sample, bool) {
	key := db.MakeKey(tag, ts)
	iter := db.db.NewIterator(&util.Range{Limit: key}, nil)
	if !iter.Last() {
		return nil, false
	}
	var sample model.Sample
	Check(db.szr.Unmarshal(iter.Value(), &sample))
	return &sample, true
}

func (db *DB) Report(tag string, start, end time.Time) []pimco.ReportLine {
	tsBegin, tsEnd := start.UnixNano(), end.UnixNano()
	step := (tsEnd - tsBegin) / 100
	var resp []pimco.ReportLine
	var prevSample *model.Sample
	for ts := tsBegin; ts < tsEnd; ts += step {
		sample, ok := db.ReportOne(tag, ts)
		if !ok {
			sample = prevSample
		} else {
			prevSample = sample
		}
		resp = append(resp, *pimco.ReportLineFromSample(sample))
	}
	return resp
}
