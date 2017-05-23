package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/galtsev/dstor"
	. "github.com/galtsev/dstor/base"
	"github.com/galtsev/dstor/conf"
	"github.com/galtsev/dstor/model"
	"github.com/galtsev/dstor/serializer"
)

type Writer struct {
	batch     []model.Sample
	producer  sarama.SyncProducer
	topic     string
	partition int32
	szr       serializer.Serializer
	writer    *pimco.BatchWriter
}

func NewWriter(cfg conf.KafkaConfig, partition int32, ctx pimco.BatchContext) *Writer {
	w := Writer{
		topic:     cfg.Topic,
		partition: partition,
		szr:       serializer.NewSerializer(cfg.Serializer),
	}
	w.writer = pimco.NewWriter(&w, cfg.Batch, ctx)
	conf := sarama.NewConfig()
	conf.Producer.Partitioner = sarama.NewManualPartitioner
	conf.Producer.Return.Successes = true
	conf.Producer.Return.Errors = true
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

func (w *Writer) AddSample(sample *model.Sample, offset int64) {
	w.writer.Write(sample, offset)
}

func (w *Writer) Close() {
	w.writer.Close()
	w.producer.Close()
}
