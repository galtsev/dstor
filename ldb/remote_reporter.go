package ldb

import (
	"dan/pimco"
	"dan/pimco/api"
	"dan/pimco/conf"
	"time"
)

type Reporter struct {
	nodes       []*api.Client
	partitioner func(string) int32
}

func NewReporter(cfg conf.Config) *Reporter {
	cluster := Reporter{
		partitioner: pimco.MakePartitioner(cfg.Kafka.NumPartitions),
	}
	// TODO: get nodes from Zookeeper
	for p := 0; p < cfg.Kafka.NumPartitions; p++ {
		client := api.NewClient(cfg.Client)
		cluster.nodes = append(cluster.nodes, client)
	}
	return &cluster
}

func (srv *Reporter) Report(tag string, start, stop time.Time) []pimco.ReportLine {
	partition := srv.partitioner(tag)
	return srv.nodes[partition].Report(tag, start, stop)
}
