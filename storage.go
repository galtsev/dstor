package pimco

import (
	"dan/pimco/model"
	"time"
)

var (
	backendRegistry = make(map[string]func(cfg Config) Backend)
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
	AddSample(sample *model.Sample)
	Close()
}

type Backend interface {
	Storage
	Reporter
}

func RegisterBackend(name string, factory func(cfg Config) Backend) {
	backendRegistry[name] = factory
}

func MakeBackend(name string, cfg Config) Backend {
	return backendRegistry[name](cfg)
}
