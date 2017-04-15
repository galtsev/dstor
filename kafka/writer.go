package kafka

import (
	. "dan/pimco/base"
	"dan/pimco/model"
	"github.com/Shopify/sarama"
	"github.com/mailru/easyjson"
)

type Writer struct {
	batch     []model.Sample
	producer  sarama.SyncProducer
	topic     string
	partition int32
}

func NewWriter(hosts []string, topic string, partition int32) *Writer {
	w := Writer{
		topic:     topic,
		partition: partition,
	}
	conf := sarama.NewConfig()
	conf.Producer.Partitioner = sarama.NewManualPartitioner
	conf.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(hosts, conf)
	Check(err)
	w.producer = producer
	return &w
}

func (w *Writer) Add(sample *model.Sample) {
	w.batch = append(w.batch, *sample)
}

func (w *Writer) Flush() {
	if len(w.batch) > 0 {
		body, err := easyjson.Marshal(model.Samples(w.batch))
		Check(err)
		msg := sarama.ProducerMessage{
			Topic:     w.topic,
			Value:     sarama.ByteEncoder(body),
			Partition: w.partition,
		}
		_, _, err = w.producer.SendMessage(&msg)
		Check(err)
		w.batch = w.batch[:0]
	}
}

func (w *Writer) Close() {
	w.Flush()
	w.producer.Close()
}
