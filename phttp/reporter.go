package phttp

import (
	"dan/pimco"
	. "dan/pimco/base"
	"time"
)

type ReportRequest struct {
	Tag   string
	Start string
	End   string
}

type ReportResponse struct {
	Tag     string             `json:"tagName"`
	Start   int64              `json:"start"`
	End     int64              `json:"end"`
	Samples []pimco.ReportLine `json:"samples"`
}

func (req ReportRequest) Period() (time.Time, time.Time) {
	start, err := time.Parse(DATE_FORMAT_LONG, req.Start)
	Check(err)
	stop, err := time.Parse(DATE_FORMAT_LONG, req.End)
	Check(err)
	return start, stop
}
