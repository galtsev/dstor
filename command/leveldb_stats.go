package command

import (
	"dan/pimco/base"
	"dan/pimco/conf"
	"dan/pimco/ldb"
	"dan/pimco/model"
	"dan/pimco/serializer"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
	"sort"
	"time"
)

type stats struct {
	h map[time.Time]int
	t map[string]int
}

type statRec struct {
	t time.Time
	n int
}

type statRecs []statRec

func (r statRecs) Len() int           { return len(r) }
func (r statRecs) Swap(i, j int)      { r[i], r[j] = r[j], r[i] }
func (r statRecs) Less(i, j int) bool { return r[i].t.Before(r[j].t) }

type tagRec struct {
	tag string
	n   int
}

type tagRecs []tagRec

func (r tagRecs) Len() int           { return len(r) }
func (r tagRecs) Swap(i, j int)      { r[i], r[j] = r[j], r[i] }
func (r tagRecs) Less(i, j int) bool { return r[i].tag < r[j].tag }

func collectStats(backend *ldb.DB, stats *stats) {
	szr := serializer.MsgPackSerializer{}
	db := backend.GetDB()
	iter := db.NewIterator(&util.Range{}, &opt.ReadOptions{DontFillCache: true})
	defer iter.Release()
	for iter.Next() {
		var sample model.Sample
		t := time.Unix(0, backend.KeyTime(iter.Key()))
		stats.h[t.Round(time.Hour)] += 1
		base.Check(szr.Unmarshal(iter.Value(), &sample))
		stats.t[sample.Tag] += 1
	}
}

func showStats(stats *stats) {
	var rstat []statRec
	for h, cnt := range stats.h {
		rstat = append(rstat, statRec{t: h.UTC(), n: cnt})
	}
	sort.Sort(statRecs(rstat))
	for _, r := range rstat {
		fmt.Printf("%v %10d\n", r.t, r.n)
	}
	var tstat []tagRec
	for tag, cnt := range stats.t {
		tstat = append(tstat, tagRec{tag, cnt})
	}
	sort.Sort(tagRecs(tstat))
	for _, r := range tstat {
		fmt.Printf("%s\t%10d\n", r.tag, r.n)
	}
}

func LeveldbStats(args []string) {
	cfg := conf.LoadConfig(args...)
	fmt.Println(cfg)
	srv := ldb.NewCluster(cfg)
	stats := stats{
		h: make(map[time.Time]int),
		t: make(map[string]int),
	}

	for n, backend := range srv.Backends() {
		fmt.Printf("Collecting stats for %d\n", n)
		collectStats(backend, &stats)
	}
	showStats(&stats)
}
