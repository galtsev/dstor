package kafka

import (
	"dan/pimco"
	. "dan/pimco/base"
	"dan/pimco/conf"
	"dan/pimco/model"
	"dan/pimco/serializer"
	"github.com/Shopify/sarama"
	"log"
)

type KafkaSample struct {
	Sample model.Sample
	Offset int64
}

func ConsumePartition(cfg conf.KafkaConfig, partition int32, offset int64, oneShot bool) chan KafkaSample {
	ch := make(chan KafkaSample, 1000)
	conf := sarama.NewConfig()
	var finalOffset int64

	client, err := sarama.NewClient(cfg.Hosts, conf)
	Check(err)
	finalOffset, err = client.GetOffset(cfg.Topic, partition, sarama.OffsetNewest)
	Check(err)

	consumer, err := sarama.NewConsumerFromClient(client)
	Check(err)
	partitionConsumer, err := consumer.ConsumePartition(cfg.Topic, partition, offset)
	Check(err)
	szr := serializer.NewSerializer(cfg.Serializer)
	log.Printf("Started consumer for partition %d, offset %d, newest offset %d", partition, offset, finalOffset)
	go func() {
		defer client.Close()
		defer consumer.Close()
		defer partitionConsumer.Close()
		for msg := range partitionConsumer.Messages() {
			var samples model.Samples
			szr.Unmarshal(msg.Value, &samples)
			for _, sample := range samples {
				ch <- KafkaSample{Sample: sample, Offset: msg.Offset}
			}
			if oneShot && msg.Offset > finalOffset-1 {
				close(ch)
				return
			}
		}
	}()
	return ch
}

func PartitionLoader(cfg conf.KafkaConfig, partition int32, offset int64, db pimco.Storage) {
	for ksample := range ConsumePartition(cfg, partition, offset, false) {
		db.AddSample(&ksample.Sample, ksample.Offset)
	}
}
