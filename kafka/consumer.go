package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/galtsev/dstor"
	. "github.com/galtsev/dstor/base"
	"github.com/galtsev/dstor/conf"
	"github.com/galtsev/dstor/model"
	"github.com/galtsev/dstor/serializer"
	"log"
	"sync"
)

type KafkaSample struct {
	Sample model.Sample
	Offset int64
}

func ConsumePartition(cfg conf.KafkaConfig, partition int32, offset int64, wg *sync.WaitGroup, oneShot bool) chan KafkaSample {
	ch := make(chan KafkaSample, 1000)
	conf := sarama.NewConfig()
	var finalOffset int64
	var done bool = false

	client, err := sarama.NewClient(cfg.Hosts, conf)
	Check(err)
	finalOffset, err = client.GetOffset(cfg.Topic, partition, sarama.OffsetNewest)
	Check(err)
	if finalOffset == 0 {
		done = true
		if wg != nil {
			wg.Done()
		}
		log.Printf("Consumer of partition %d start with zero HighWaterMark", partition)
	}

	consumer, err := sarama.NewConsumerFromClient(client)
	Check(err)
	partitionConsumer, err := consumer.ConsumePartition(cfg.Topic, partition, offset)
	Check(err)
	szr := serializer.NewSerializer(cfg.Serializer)
	log.Printf("Started consumer for partition %d, offset %d, newest offset %d, oneshot: %v", partition, offset, finalOffset, oneShot)
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
			if wg != nil && !done && msg.Offset+cfg.DoneOffset >= partitionConsumer.HighWaterMarkOffset() {
				wg.Done()
				done = true
				log.Printf("Consumer of partition %d reach HighWaterMark. Current offset: %d, newest: %d", partition, msg.Offset, partitionConsumer.HighWaterMarkOffset())
			}
			if oneShot && msg.Offset >= finalOffset-1 {
				close(ch)
				log.Printf("consumer for partition %d finished", partition)
				return
			}
		}
	}()
	return ch
}

func PartitionLoader(cfg conf.KafkaConfig, partition int32, offset int64, db pimco.Storage, wg *sync.WaitGroup) {
	for ksample := range ConsumePartition(cfg, partition, offset, wg, false) {
		db.AddSample(&ksample.Sample, ksample.Offset)
	}
}
