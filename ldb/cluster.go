package ldb

import (
	"dan/pimco"
	"dan/pimco/conf"
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

type ClusterContext interface {
	OnFlush(partition int32, offset int64)
}

type partitionContext struct {
	*TagIndex
	partition int32
	ctx       ClusterContext
}

func (ctx partitionContext) OnFlush(offset int64) {
	ctx.ctx.OnFlush(ctx.partition, offset)
}

func NewCluster(cfg conf.LeveldbConfig, ctx ClusterContext) *LeveldbCluster {
	server := LeveldbCluster{
		partitioner: pimco.MakePartitioner(cfg.NumPartitions),
		tagIndex:    NewTagIndex(path.Join(cfg.Path, "tags")),
	}
	for _, p := range cfg.Partitions {
		ctx := partitionContext{
			TagIndex:  server.tagIndex,
			partition: int32(p),
			ctx:       ctx,
		}
		db := Open(cfg, p, ctx)
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

func (srv *LeveldbCluster) AddSample(sample *model.Sample, offset int64) {
	srv.backends[srv.partitioner(sample.Tag)].AddSample(sample, offset)
	pimco.SetLatest(sample.TS)
}

func (srv *LeveldbCluster) Report(tag string, start, stop time.Time) []pimco.ReportLine {
	return srv.backends[srv.partitioner(tag)].Report(tag, start, stop)
}

func (srv *LeveldbCluster) Backends() []*DB {
	return srv.backends
}
