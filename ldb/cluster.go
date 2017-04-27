package ldb

import (
	"dan/pimco"
	"dan/pimco/model"
	"dan/pimco/serializer"
	"path"
	"time"
)

type LeveldbCluster struct {
	backends    []*DB
	tagIndex    *TagIndex
	json        serializer.Serializer
	partitioner func(string) int32
}

func NewCluster(cfg pimco.Config) *LeveldbCluster {
	server := LeveldbCluster{
		json:        serializer.NewSerializer("easyjson"),
		partitioner: pimco.MakePartitioner(cfg.Kafka.NumPartitions),
		tagIndex:    NewTagIndex(path.Join(cfg.Leveldb.Path, "tags")),
	}
	opts := Options{
		Leveldb:  cfg.Leveldb,
		Batch:    cfg.Batch,
		TagIndex: server.tagIndex,
	}
	for p := 0; p < cfg.Kafka.NumPartitions; p++ {
		db := Open(int32(p), &opts)
		server.backends = append(server.backends, db)
	}
	return &server
}

func (srv *LeveldbCluster) Close() {
	for _, w := range srv.backends {
		w.Close()
	}
	srv.tagIndex.Close()
}

func (srv *LeveldbCluster) AddSample(sample *model.Sample) {
	srv.backends[srv.partitioner(sample.Tag)].AddSample(sample)
	pimco.SetLatest(sample.TS)
}

func (srv *LeveldbCluster) Report(tag string, start, stop time.Time) []pimco.ReportLine {
	return srv.backends[srv.partitioner(tag)].Report(tag, start, stop)
}

func (srv *LeveldbCluster) Backends() []*DB {
	return srv.backends
}

func init() {
	pimco.RegisterBackend("leveldb", func(cfg pimco.Config) pimco.Backend {
		return NewCluster(cfg)
	})
}
