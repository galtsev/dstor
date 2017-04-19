package phttp

import (
	"dan/pimco"
	. "dan/pimco/base"
	"dan/pimco/kafka"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"time"
)

type ReportRequest struct {
	Tag   string
	Start string
	End   string
}

func (req ReportRequest) Period() (time.Time, time.Time) {
	start, err := time.Parse(DATE_FORMAT_LONG, req.Start)
	Check(err)
	stop, err := time.Parse(DATE_FORMAT_LONG, req.End)
	Check(err)
	return start, stop
}

func MakeReportHandler(db pimco.Reporter) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req ReportRequest
		body, err := ioutil.ReadAll(r.Body)
		Check(err)
		Check(json.Unmarshal(body, &req))
		start, stop := req.Period()
		lines := db.Report(req.Tag, start, stop)
		body, err = json.Marshal(lines)
		Check(err)
		w.Header().Add("Content-Type", "application/json")
		w.Write(body)
	})
}

func Serve(cfg pimco.Config, db pimco.Storage) {
	for _, partition := range cfg.Kafka.Partitions {
		go kafka.PartitionLoader(cfg, partition, db)
	}
	http.Handle("/report", MakeReportHandler(db))
	err := http.ListenAndServe(cfg.ReportingServer.Addr, nil)
	Check(err)
}
