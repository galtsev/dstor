package pimco

import (
	"dan/pimco/model"
	"time"
)

type ReportLine struct {
	TS     int64
	Values [10]float64
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

/*
type FakeReporter struct{}

func (r FakeReporter) Report(tag string, start, stop time.Time) []ReportLine {
	return []ReportLine{}
}

type FakeStorage struct{}

func (s FakeStorage) AddSample(sample *model.Sample, offset int64) {}

func (s FakeStorage) Close() {}
*/
