package temp

import (
	"dan/pimco"
	. "dan/pimco/base"
	"dan/pimco/kafka"
	"dan/pimco/model"
	"fmt"
	"github.com/Workiva/go-datastructures/common"
	"github.com/Workiva/go-datastructures/slice/skip"
	"github.com/mailru/easyjson"
	"io"
	"log"
	"net/http"
	"strconv"
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

func NewDB() *DB {
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

func (db *DB) Report(tag string, ts int64) (*model.Sample, bool) {
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

func (db *DB) ScanTag(tag string, w io.Writer) {
	list := db.Tags[tag]
	start := model.Sample{TS: 3491472800400000000}
	iter := list.Iter((*xSample)(&start))
	cnt := 0
	prevCnt := 0
	for iter.Next() {
		cnt++
		if cnt%1000 == 0 {
			sample := (*model.Sample)(iter.Value().(*xSample))
			fmt.Fprintf(w, "%d %d\n", sample.TS, cnt-prevCnt)
			prevCnt = cnt
		}
	}
}

func partitionLoader(cfg pimco.Config, partition int32, db *DB) {
	cnt := 0
	for sample := range kafka.ConsumePartition(cfg.Kafka, partition, false) {
		db.AddSample(&sample)
		cnt++
		if cnt%10000 == 0 {
			log.Printf("read %d samples", cnt)
		}
	}
}

func makeReportHandler(db *DB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tag := r.FormValue("tag")
		ts, err := strconv.Atoi(r.FormValue("ts"))
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		respData, ok := db.Report(tag, int64(ts))
		if !ok {
			respData = &model.Sample{}
		}
		body, err := easyjson.Marshal(respData)
		Check(err)
		w.Write(body)
	})
}

func scanTag(db *DB) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tag := r.FormValue("tag")
		log.Printf("scantag %s", tag)
		db.ScanTag(tag, w)
	})
}

func MemServer(args []string) {
	cfg := pimco.LoadConfig(args...)
	log.Println(cfg)
	db := NewDB()
	for _, partition := range cfg.Kafka.Partitions {
		go partitionLoader(cfg, partition, db)
	}
	http.Handle("/report", makeReportHandler(db))
	http.Handle("/scan", scanTag(db))
	err := http.ListenAndServe(cfg.HTTPServer.Addr, nil)
	Check(err)
}
