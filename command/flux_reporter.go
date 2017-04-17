package command

import (
	"dan/pimco"
	. "dan/pimco/base"
	"dan/pimco/influx"
	"dan/pimco/phttp"
	"log"
	"net/http"
)

func FluxServer(args []string) {
	cfg := pimco.LoadConfig(args...)
	log.Println(cfg)
	db := influx.NewReporter(cfg.Influx)
	http.Handle("/report", phttp.MakeReportHandler(db))
	err := http.ListenAndServe(cfg.ReportingServer.Addr, nil)
	Check(err)
}
