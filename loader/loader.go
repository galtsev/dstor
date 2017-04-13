package loader

import (
	"dan/pimco/model"
	. "dan/pimco/util"
	"flag"
	"github.com/Shopify/sarama"
	"github.com/influxdata/influxdb/client/v2"

	"github.com/mailru/easyjson"
	"log"
	"strconv"
	"strings"
	"sync"
)

type Config struct {
	Influx struct {
		URL      string
		Database string
	}
	Kafka struct {
		Hosts      []string
		Topic      string
		Partitions []int32
	}
	BS      int // save points to influx in batches of BS size
	OneShot bool
}

func addPoints(batch client.BatchPoints, data []byte) int {
	var samples model.Samples
	err := easyjson.Unmarshal(data, &samples)
	Check(err)
	for _, sample := range samples {
		AddSample(&sample, batch)
	}
	return len(samples)
}

func consume(consumer sarama.PartitionConsumer, cfg Config) {
	defer consumer.Close()
	highWaterMark := consumer.HighWaterMarkOffset()
	flux, err := client.NewHTTPClient(client.HTTPConfig{Addr: cfg.Influx.URL})
	Check(err)
	defer flux.Close()
	batchPointsConfig := client.BatchPointsConfig{
		Database: cfg.Influx.Database,
	}
	cnt := 0
	for {
		msg, ok := <-consumer.Messages()
		if !ok {
			return
		}
		batch, err := client.NewBatchPoints(batchPointsConfig)
		Check(err)
		cnt += addPoints(batch, msg.Value)
		lastOffset := msg.Offset
		stop := false
		for !stop && len(batch.Points()) < cfg.BS {
			select {
			case msg, ok := <-consumer.Messages():
				if !ok {
					//TODO : handle this
					stop = true
				} else {
					cnt += addPoints(batch, msg.Value)
					lastOffset = msg.Offset
				}
			default:
				stop = true

			}
		}
		Check(flux.Write(batch))
		if cfg.OneShot && lastOffset+1 >= highWaterMark {
			log.Printf("loaded %d messages", cnt)
			return
		}
	}

}

func Run(args []string) {
	var wg sync.WaitGroup
	var cfg Config
	fs := flag.NewFlagSet("loader", flag.ContinueOnError)
	fs.StringVar(&cfg.Influx.URL, "influx-addr", "http://localhost:8086", "Influxdb url")
	fs.StringVar(&cfg.Influx.Database, "db", "test", "Influxdb database name")
	fs.StringVar(&cfg.Kafka.Topic, "topic", "test", "Kafka topic")
	fs.IntVar(&cfg.BS, "bs", 1000, "Influx write batch size")
	fs.BoolVar(&cfg.OneShot, "oneshot", false, "Dont wait for new messages, exit")
	hosts := fs.String("kafka-hosts", "localhost:9092", "Kafka hosts")
	partitions := fs.String("partitions", "", "Comma-separated list of partitions")
	err := fs.Parse(args)
	if err != nil {
		log.Fatal(err)
	}
	cfg.Kafka.Hosts = strings.Split(*hosts, ",")
	for _, ps := range strings.Split(*partitions, ",") {
		pnum, err := strconv.Atoi(ps)
		Check(err)
		cfg.Kafka.Partitions = append(cfg.Kafka.Partitions, int32(pnum))
	}
	conf := sarama.NewConfig()
	consumer, err := sarama.NewConsumer(cfg.Kafka.Hosts, conf)
	Check(err)
	for _, p := range cfg.Kafka.Partitions {
		// TODO: save/load offset
		partitionConsumer, err := consumer.ConsumePartition(cfg.Kafka.Topic, p, 0)
		Check(err)
		wg.Add(1)
		go func(pc sarama.PartitionConsumer) {
			consume(pc, cfg)
			wg.Done()
		}(partitionConsumer)
	}
	wg.Wait()

}
