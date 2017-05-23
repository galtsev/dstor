package pimco

import (
	"github.com/galtsev/dstor/model"
	"time"
)

type ReportLine struct {
	TS     int64       `json:"time"`
	Values [10]float64 `json:"values"`
}

func ReportLineFromSample(sample *model.Sample) *ReportLine {
	line := ReportLine{
		TS:     sample.TS,
		Values: sample.Values,
	}
	return &line
}

type Reporter interface {
	Report(tag string, start, stop time.Time) []ReportLine
}

type Storage interface {
	AddSample(sample *model.Sample, offset int64)
	Close()
}

type Backend interface {
	Storage
	Reporter
}

type NodeId interface {
	NodeId() string
}

type OffsetStorage interface {
	GetOffset(partition int32) int64
	OnFlush(partition int32, offset int64)
}
