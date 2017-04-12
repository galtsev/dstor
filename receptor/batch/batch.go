package batch

import (
	"dan/pimco/model"
	. "dan/pimco/util"
	"flag"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/mailru/easyjson"
	"github.com/valyala/fasthttp"
	"hash/fnv"
	"log"
	"os"
	"strings"
	"sync"
)

var (
	FIELD_NAMES = []string{"V1", "V2", "V3", "V4", "V5", "V6", "V7", "V8", "V9", "V10"}
	EMPTY_TAGS  = map[string]string{}
)

type Config struct {
	Kafka struct {
		Hosts []string
		Topic string
	}
	Addr      string
	BatchSize int
}

func partitionFunc(numPartitions int) func(string) int {
	hash := fnv.New32a()
	return func(key string) int {
		hash.Reset()
		hash.Write([]byte(key))
		return int(hash.Sum32()) % numPartitions
	}
}

func makeHandler(pmap []chan model.Sample) fasthttp.RequestHandler {
	partitioner := partitionFunc(len(pmap))
	return func(ctx *fasthttp.RequestCtx) {
		var samples model.Samples
		err := easyjson.Unmarshal(ctx.PostBody(), &samples)
		if err != nil {
			ctx.Error(err.Error(), fasthttp.StatusBadRequest)
			return
		}
		for _, sample := range samples {
			pmap[partitioner(sample.Tag)] <- sample
		}
		ctx.SetStatusCode(fasthttp.StatusNoContent)
	}
}

func collect(ch chan model.Sample, samples []model.Sample, batchSize int) []model.Sample {
	for len(samples) < batchSize {
		select {
		case sample, ok := <-ch:
			if ok {
				samples = append(samples, sample)
			} else {
				return samples
			}
		default:
			return samples
		}
	}
	return samples
}

func saveLoop(ch chan model.Sample, cfg Config, partition int32) {
	conf := sarama.NewConfig()
	conf.Producer.Partitioner = sarama.NewManualPartitioner
	producer, err := sarama.NewSyncProducer(cfg.Kafka.Hosts, conf)
	Check(err)
	log.Println("Producer started")
	for {
		sample, ok := <-ch
		if !ok {
			return
		}
		samples := make([]model.Sample, 0, cfg.BatchSize)
		samples = append(samples, sample)
		samples = collect(ch, samples, cfg.BatchSize)
		body, err := easyjson.Marshal(model.Samples(samples))
		if err != nil {
			log.Print(err)
			continue
		}
		msg := sarama.ProducerMessage{
			Topic:     cfg.Kafka.Topic,
			Value:     sarama.ByteEncoder(body),
			Partition: partition,
		}
		_, _, err = producer.SendMessage(&msg)
		if err != nil {
			log.Print(err)
		}
	}
}

func getPartitions(cfg Config) int {
	conf := sarama.NewConfig()
	client, err := sarama.NewClient(cfg.Kafka.Hosts, conf)
	Check(err)
	partitions, err := client.Partitions(cfg.Kafka.Topic)
	Check(err)
	return len(partitions)
}

func Run(args []string) {
	var cfg Config
	var wg sync.WaitGroup
	fs := flag.NewFlagSet("kafka", flag.ContinueOnError)
	fs.StringVar(&cfg.Kafka.Topic, "topic", "test", "Kafka topic")
	hosts := fs.String("hosts", "192.168.0.2:9092", "Comma-separated list of Kafka hosts")
	fs.StringVar(&cfg.Addr, "addr", "localhost:9876", "Serve address:port")
	fs.IntVar(&cfg.BatchSize, "bs", 100, "Batch size")
	err := fs.Parse(args)
	if err != nil {
		fmt.Print(err)
		os.Exit(2)
	}
	cfg.Kafka.Hosts = strings.Split(*hosts, ",")
	numPartitions := getPartitions(cfg)
	log.Printf("Num partitions: %d", numPartitions)
	pmap := make([]chan model.Sample, numPartitions)
	wg.Add(numPartitions)
	for partition := 0; partition < numPartitions; partition++ {
		ch := make(chan model.Sample, 1000)
		pmap[partition] = ch
		go func(ch chan model.Sample, p int) {
			saveLoop(ch, cfg, int32(p))
			wg.Done()
		}(ch, partition)
	}
	err = fasthttp.ListenAndServe(cfg.Addr, makeHandler(pmap))
	log.Print(err)
	for _, ch := range pmap {
		close(ch)
	}
	wg.Wait()
}
