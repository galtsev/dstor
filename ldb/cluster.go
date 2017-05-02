package ldb

import (
	"dan/pimco"
	"dan/pimco/conf"
	"dan/pimco/model"
	"time"
)

type LeveldbCluster struct {
	backends    []*DB
	partitioner func(string) int32
}

func NewCluster(cfg conf.Config) *LeveldbCluster {
	server := LeveldbCluster{
		partitioner: pimco.MakePartitioner(cfg.Kafka.NumPartitions),
	}
	for p := 0; p < cfg.Kafka.NumPartitions; p++ {
		db := Open(cfg.Leveldb, int32(p))
		server.backends = append(server.backends, db)
	}
	return &server
}

func (srv *LeveldbCluster) Close() {
	for _, w := range srv.backends {
		w.Close()
	}
}

func (srv *LeveldbCluster) AddSample(sample *model.Sample, offset int64) {
	srv.backends[srv.partitioner(sample.Tag)].AddSample(sample, offset)
}

func (srv *LeveldbCluster) Report(tag string, start, stop time.Time) []pimco.ReportLine {
	return srv.backends[srv.partitioner(tag)].Report(tag, start, stop)
}

func (srv *LeveldbCluster) Backends() []*DB {
	return srv.backends
}

func init() {
	pimco.RegisterBackend("leveldb", func(cfg conf.Config) pimco.Backend {
		return NewCluster(cfg)
	})
}
