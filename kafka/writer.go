package kafka

import (
	"dan/pimco"
	. "dan/pimco/base"
	"dan/pimco/conf"
	"dan/pimco/model"
	"dan/pimco/serializer"
	"github.com/Shopify/sarama"
)

type Writer struct {
	batch     []model.Sample
	producer  sarama.SyncProducer
	topic     string
	partition int32
	szr       serializer.Serializer
	writer    *pimco.BatchWriter
}

func NewWriter(cfg conf.KafkaConfig, partition int32) *Writer {
	w := Writer{
		topic:     cfg.Topic,
		partition: partition,
		szr:       serializer.NewSerializer(cfg.Serializer),
	}
	w.writer = pimco.NewWriter(&w, cfg.Batch)
	conf := sarama.NewConfig()
	conf.Producer.Partitioner = sarama.NewManualPartitioner
	conf.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(cfg.Hosts, conf)
	Check(err)
	w.producer = producer
	return &w
}

func (w *Writer) Add(sample *model.Sample) {
	w.batch = append(w.batch, *sample)
}

func (w *Writer) Flush() {
	if len(w.batch) > 0 {
		body := w.szr.Marshal(model.Samples(w.batch))
		msg := sarama.ProducerMessage{
			Topic:     w.topic,
			Value:     sarama.ByteEncoder(body),
			Partition: w.partition,
		}
		_, _, err := w.producer.SendMessage(&msg)
		Check(err)
		w.batch = w.batch[:0]
	}
}

func (w *Writer) AddSample(sample *model.Sample) {
	w.writer.Write(sample)
}

func (w *Writer) Close() {
	w.Flush()
	w.producer.Close()
}
