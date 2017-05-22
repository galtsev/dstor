package ldb

import (
	"dan/pimco"
	"dan/pimco/api"
	"dan/pimco/conf"
	"sync"
	"time"
)

type Registry interface {
	GetReporter(partition int32) string
}

type Reporter struct {
	sync.Mutex
	nodes       map[string]*api.Client
	partitioner func(string) int32
	registry    Registry
}

func NewReporter(cfg conf.Config, registry Registry) *Reporter {
	cluster := Reporter{
		partitioner: pimco.MakePartitioner(cfg.Kafka.NumPartitions),
		registry:    registry,
		nodes:       make(map[string]*api.Client),
	}
	return &cluster
}

func (srv *Reporter) GetClient(partition int32) *api.Client {
	srv.Lock()
	defer srv.Unlock()
	host := srv.registry.GetReporter(partition)
	if host == "" {
		return nil
	}
	client, ok := srv.nodes[host]
	if !ok {
		cfg := conf.ClientConfig{
			Host: host,
		}
		client := api.NewClient(cfg)
		srv.nodes[host] = client
	}
	return client
}

func (srv *Reporter) Report(tag string, start, stop time.Time) []pimco.ReportLine {
	emptyReport := []pimco.ReportLine{}
	client := srv.GetClient(srv.partitioner(tag))
	if client == nil {
		return emptyReport
	} else {
		return client.Report(tag, start, stop)
	}
}
