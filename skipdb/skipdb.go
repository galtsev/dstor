package skipdb

import (
	"github.com/Workiva/go-datastructures/common"
	"github.com/Workiva/go-datastructures/slice/skip"
	"github.com/galtsev/dstor"
	"github.com/galtsev/dstor/model"
	"time"
)

type xSample model.Sample

/* Compare in reverse order, as we need to lookup last sample before specified timestamp
 */
func (s *xSample) Compare(other common.Comparator) int {
	v := other.(*xSample)
	return int(v.TS - s.TS)
}

type DB struct {
	Tags map[string]*skip.SkipList
}

func New() *DB {
	db := &DB{
		Tags: make(map[string]*skip.SkipList),
	}
	return db
}

func (db *DB) AddSample(sample *model.Sample) {
	s := *sample // isolate storage from sender
	list, ok := db.Tags[s.Tag]
	if !ok {
		list = skip.New(uint64(0))
		db.Tags[s.Tag] = list
	}
	list.Insert((*xSample)(&s))
}

func (db *DB) ReportOne(tag string, ts int64) (*model.Sample, bool) {
	list, ok := db.Tags[tag]
	if !ok {
		return nil, false
	}
	iter := list.Iter((*xSample)(&model.Sample{TS: ts}))
	if iter.Next() {
		return (*model.Sample)(iter.Value().(*xSample)), true
	} else {
		return nil, false
	}
}

func (db *DB) Report(tag string, start, end time.Time) []dstor.ReportLine {
	tsBegin, tsEnd := start.UnixNano(), end.UnixNano()
	step := (tsEnd - tsBegin) / 100
	var resp []dstor.ReportLine
	var prevSample *model.Sample
	for ts := tsBegin; ts < tsEnd; ts += step {
		sample, ok := db.ReportOne(tag, ts)
		if !ok {
			sample = prevSample
		} else {
			prevSample = sample
		}
		resp = append(resp, *dstor.ReportLineFromSample(sample))
	}
	return resp
}

func (db *DB) Close() {

}
