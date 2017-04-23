package phttp

import (
	. "dan/pimco/base"
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
