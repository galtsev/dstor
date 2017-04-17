package phttp

import (
	"dan/pimco"
	. "dan/pimco/base"
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
	//Mon Jan 2 15:04:05 -0700 MST 2006
	date_format := "2006-01-02 15:04:05"
	start, err := time.Parse(date_format, req.Start)
	Check(err)
	stop, err := time.Parse(date_format, req.End)
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
