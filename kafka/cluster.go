package kafka

import (
	"github.com/galtsev/dstor"
	"github.com/galtsev/dstor/conf"
	"github.com/galtsev/dstor/model"
)

type KafkaCluster struct {
	writers     []*Writer
	partitioner func(string) int32
}

func NewCluster(cfg conf.Config) *KafkaCluster {
	cluster := KafkaCluster{
		partitioner: dstor.MakePartitioner(cfg.Kafka.NumPartitions),
	}
	for p := 0; p < cfg.Kafka.NumPartitions; p++ {
		w := NewWriter(cfg.Kafka, int32(p), dstor.FakeContext{})
		cluster.writers = append(cluster.writers, w)
	}
	return &cluster
}

func (srv *KafkaCluster) Close() {
	for _, w := range srv.writers {
		w.Close()
	}
}

func (srv *KafkaCluster) AddSample(sample *model.Sample, offset int64) {
	srv.writers[srv.partitioner(sample.Tag)].AddSample(sample, offset)
}
