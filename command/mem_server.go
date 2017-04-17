package command

import (
	"dan/pimco"
	. "dan/pimco/base"
	"dan/pimco/kafka"
	"dan/pimco/phttp"
	"dan/pimco/skipdb"
	"log"
	"net/http"
)

func partitionLoader(cfg pimco.Config, partition int32, db *skipdb.DB) {
	cnt := 0
	for sample := range kafka.ConsumePartition(cfg.Kafka, partition, false) {
		db.AddSample(&sample)
		cnt++
		if cnt%10000 == 0 {
			log.Printf("read %d samples", cnt)
		}
	}
}

func MemServer(args []string) {
	cfg := pimco.LoadConfig(args...)
	log.Println(cfg)
	db := skipdb.NewDB()
	for _, partition := range cfg.Kafka.Partitions {
		go partitionLoader(cfg, partition, db)
	}
	http.Handle("/report", phttp.MakeReportHandler(db))
	err := http.ListenAndServe(cfg.ReportingServer.Addr, nil)
	Check(err)
}
