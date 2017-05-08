package ldb

import (
	"dan/pimco"
	. "dan/pimco/base"
	"dan/pimco/conf"
	"dan/pimco/model"
	"dan/pimco/serializer"
	"dan/pimco/util"
	"io/ioutil"
	"os"
	"path"
	"time"
)

type LeveldbCluster struct {
	backends    []*DB
	tagIndex    *TagIndex
	json        serializer.Serializer
	partitioner func(string) int32
	nodeId      string
}

type ClusterContext interface {
	pimco.OffsetStorage
}

type partitionContext struct {
	*TagIndex
	partition int32
	ctx       ClusterContext
}

func (ctx partitionContext) OnFlush(offset int64) {
	if ctx.ctx != nil {
		ctx.ctx.OnFlush(ctx.partition, offset)
	}
}

func NewCluster(cfg conf.LeveldbConfig, ctx ClusterContext) *LeveldbCluster {
	server := LeveldbCluster{
		partitioner: pimco.MakePartitioner(cfg.NumPartitions),
		tagIndex:    NewTagIndex(path.Join(cfg.Path, "tags")),
	}

	// get nodeId, create if missing
	metaPath := path.Join(cfg.Path, "node_id")
	if _, err := os.Stat(metaPath); os.IsNotExist(err) {
		server.nodeId = util.NewUID()
		Check(ioutil.WriteFile(metaPath, []byte(server.nodeId), os.FileMode(0640)))
	} else {
		buf, err := ioutil.ReadFile(metaPath)
		Check(err)
		server.nodeId = string(buf)
	}

	for _, p := range cfg.Partitions {
		pctx := partitionContext{
			TagIndex:  server.tagIndex,
			partition: int32(p),
			ctx:       ctx,
		}
		db := Open(cfg, p, pctx)
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

func (srv *LeveldbCluster) NodeId() string {
	return srv.nodeId
}
