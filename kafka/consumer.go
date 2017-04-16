package kafka

import (
	"dan/pimco"
	. "dan/pimco/base"
	"dan/pimco/model"
	"dan/pimco/serializer"
	"github.com/Shopify/sarama"
)

func ConsumePartition(cfg pimco.KafkaConfig, partition int32, oneShot bool) chan model.Sample {
	ch := make(chan model.Sample, 1000)
	conf := sarama.NewConfig()
	var finalOffset int64

	if oneShot {
		client, err := sarama.NewClient(cfg.Hosts, conf)
		Check(err)
		finalOffset, err = client.GetOffset(cfg.Topic, int32(0), sarama.OffsetNewest)
		Check(err)
		client.Close()
	}

	consumer, err := sarama.NewConsumer(cfg.Hosts, conf)
	Check(err)
	partitionConsumer, err := consumer.ConsumePartition(cfg.Topic, partition, 0)
	Check(err)
	szr := serializer.NewSerializer(cfg.Serializer)
	go func() {
		defer consumer.Close()
		defer partitionConsumer.Close()
		for msg := range partitionConsumer.Messages() {
			var samples model.Samples
			szr.Unmarshal(msg.Value, &samples)
			for _, sample := range samples {
				ch <- sample
			}
			if oneShot && msg.Offset+1 >= finalOffset {
				close(ch)
				return
			}
		}
	}()
	return ch
}
