package ldb

import (
	"dan/pimco"
	. "dan/pimco/base"
	"dan/pimco/conf"
	"dan/pimco/model"
	"dan/pimco/serializer"
	"encoding/binary"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
	"time"
)

var BigEndian = binary.BigEndian

type DB struct {
	db       *leveldb.DB
	szr      serializer.Serializer
	tagIndex *TagIndex
	batch    leveldb.Batch
	writer   *pimco.BatchWriter
}

func (db *DB) GetDB() *leveldb.DB {
	return db.db
}

func nowFunc() time.Time {
	return time.Unix(0, pimco.GetLatest()).Add(-time.Duration(24) * time.Hour)
}

func Open(cfg conf.LeveldbConfig, partition int32) *DB {
	opts := opt.Options{
		WriteBuffer:                   cfg.Opts.WriteBufferMb * opt.MiB,
		CompactionTableSize:           cfg.Opts.CompactionTableSizeMb * opt.MiB,
		CompactionTotalSize:           cfg.Opts.CompactionTotalSizeMb * opt.MiB,
		CompactionTotalSizeMultiplier: cfg.Opts.CompactionTotalSizeMultiplier,
		WriteL0SlowdownTrigger:        cfg.Opts.WriteL0SlowdownTrigger,
		WriteL0PauseTrigger:           cfg.Opts.WriteL0PauseTrigger,
		ExpireBefore:                  nowFunc,
	}
	partitionPath := fmt.Sprintf("%s/%d", cfg.Path, partition)
	ldb, err := leveldb.OpenFile(partitionPath, &opts)
	Check(err)
	db := DB{
		db:       ldb,
		szr:      serializer.MsgPackSerializer{},
		tagIndex: cfg.TagIndex.(*TagIndex),
	}
	db.writer = pimco.NewWriter(&db, cfg.Batch)
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

func (db *DB) AddSample(sample *model.Sample, offset int64) {
	db.writer.Write(sample, offset)
}

func (db *DB) MakeKey(tag string, ts int64) []byte {
	var res [12]byte
	ti := db.tagIndex.Get(tag)
	BigEndian.PutUint32(res[:4], uint32(ti))
	BigEndian.PutUint64(res[4:], uint64(ts))
	return res[:]
}

func (db *DB) ParseKey(key []byte) (string, time.Time) {
	t := BigEndian.Uint64(key[4:])
	return "", time.Unix(0, int64(t))
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
	var prevSample *model.Sample = &model.Sample{}
	for ts := tsBegin; ts < tsEnd; ts += step {
		sample, ok := db.ReportOne(tag, ts+step)
		if !ok {
			sample = prevSample
		} else {
			prevSample = sample
		}
		resp = append(resp, *pimco.ReportLineFromSample(sample))
	}
	return resp
}
