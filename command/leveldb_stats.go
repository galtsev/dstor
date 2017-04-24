package command

import (
	"dan/pimco"
	"dan/pimco/ldb"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
	"sort"
	"time"
)

type statRec struct {
	t time.Time
	n int
}

type statRecs []statRec

func (r statRecs) Len() int           { return len(r) }
func (r statRecs) Swap(i, j int)      { r[i], r[j] = r[j], r[i] }
func (r statRecs) Less(i, j int) bool { return r[i].t.Before(r[j].t) }

func showStats(backend *ldb.DB) {
	db := backend.GetDB()
	stats := make(map[time.Time]int)
	iter := db.NewIterator(&util.Range{}, &opt.ReadOptions{DontFillCache: true})
	defer iter.Release()
	for iter.Next() {
		_, t := backend.ParseKey(iter.Key())
		stats[t.Round(time.Hour)] += 1
	}
	var rstat []statRec
	for h, cnt := range stats {
		rstat = append(rstat, statRec{t: h.UTC(), n: cnt})
	}
	sort.Sort(statRecs(rstat))
	for _, r := range rstat {
		fmt.Printf("%v %10d\n", r.t, r.n)
	}
}

func LeveldbStats(args []string) {
	cfg := pimco.LoadConfig(args...)
	fmt.Println(cfg)
	srv := ldb.NewCluster(cfg)

	for n, backend := range srv.Backends() {
		fmt.Printf("Stats for %d\n", n)
		showStats(backend)
	}
}
