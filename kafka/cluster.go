package kafka

import (
	"dan/pimco"
	"dan/pimco/conf"
	"dan/pimco/model"
)

type KafkaCluster struct {
	pimco.FakeReporter
	writers     []*Writer
	partitioner func(string) int32
}

func NewCluster(cfg conf.Config) *KafkaCluster {
	cluster := KafkaCluster{
		partitioner: pimco.MakePartitioner(cfg.Kafka.NumPartitions),
	}
	for p := 0; p < cfg.Kafka.NumPartitions; p++ {
		w := NewWriter(cfg.Kafka, int32(p))
		cluster.writers = append(cluster.writers, w)
	}
	return &cluster
}

func (srv *KafkaCluster) Close() {
	for _, w := range srv.writers {
		w.Close()
	}
}

func (srv *KafkaCluster) AddSample(sample *model.Sample) {
	srv.writers[srv.partitioner(sample.Tag)].AddSample(sample)
}

func init() {
	pimco.RegisterBackend("kafka", func(cfg conf.Config) pimco.Backend {
		return NewCluster(cfg)
	})
}
