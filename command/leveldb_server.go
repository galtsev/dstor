package command

import (
	"dan/pimco"
	. "dan/pimco/base"
	"dan/pimco/kafka"
	"dan/pimco/ldb"
	"dan/pimco/phttp"
	"log"
	"net/http"
)

func ldbPartitionLoader(cfg pimco.Config, partition int32, db *ldb.DB) {
	cnt := 0
	for sample := range kafka.ConsumePartition(cfg.Kafka, partition, false) {
		db.AddSample(&sample)
		cnt++
		if cnt%10000 == 0 {
			log.Printf("read %d samples", cnt)
		}
	}
}

func LeveldbServer(args []string) {
	cfg := pimco.LoadConfig(args...)
	log.Println(cfg)
	db := ldb.Open(cfg.Leveldb)
	for _, partition := range cfg.Kafka.Partitions {
		go ldbPartitionLoader(cfg, partition, db)
	}
	http.Handle("/report", phttp.MakeReportHandler(db))
	err := http.ListenAndServe(cfg.ReportingServer.Addr, nil)
	Check(err)
}
