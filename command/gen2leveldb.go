package command

import (
	"container/heap"
	"dan/pimco"
	"dan/pimco/ldb"
	"dan/pimco/util"
	"fmt"
	"log"
	"time"
)

func Gen2Leveldb(args []string) {
	cfg := pimco.LoadConfig(args...)
	fmt.Println(cfg)
	db := ldb.Open(cfg.Leveldb, int32(0))
	gen := pimco.NewGenerator(cfg.Gen)
	h := &util.DurationHeap{}
	heap.Init(h)
	heapCap := 20
	cnt := 0
	startTime := time.Now()
	stx := time.Now()
	for gen.Next() {
		db.Add(gen.Sample())
		cnt++
		if cnt%1000 == 0 {
			db.Flush()
			d := time.Since(startTime)
			heap.Push(h, d)
			if cnt/1000 > heapCap {
				heap.Pop(h)
			}
			//time.Sleep(time.Duration(10) * time.Millisecond)
			startTime = time.Now()
		}
		if cnt%20000 == 0 {
			log.Printf("%12d %12d", cnt, int(time.Since(stx))/1000000)
			stx = time.Now()
		}
	}
	db.Close()
	hl := h.Len()
	for i := 0; i < hl; i++ {
		fmt.Println(heap.Pop(h).(time.Duration))
	}
}
