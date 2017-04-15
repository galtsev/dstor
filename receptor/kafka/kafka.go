package kafka

import (
	"context"
	. "dan/pimco/base"
	"dan/pimco/model"
	"flag"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/mailru/easyjson"
	"github.com/valyala/fasthttp"
	"log"
	"os"
	"strings"
)

type Config struct {
	Kafka struct {
		Hosts []string
		Topic string
	}
	Addr string
}

func makeHandler(ch chan model.Sample) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		//log.Printf("new request")
		var samples model.Samples
		err := easyjson.Unmarshal(ctx.PostBody(), &samples)
		if err != nil {
			ctx.Error(err.Error(), fasthttp.StatusBadRequest)
			return
		}
		//log.Printf("Queued: tag: %s, values: %v", sample.Tag, sample.Values)
		for _, sample := range samples {
			ch <- sample
		}
		ctx.SetStatusCode(fasthttp.StatusNoContent)
	}
}

func saveLoop(ctx context.Context, ch chan model.Sample, cfg Config) {
	conf := sarama.NewConfig()
	producer, err := sarama.NewAsyncProducer(cfg.Kafka.Hosts, conf)
	Check(err)
	log.Println("Producer started")
	for {
		select {
		case err := <-producer.Errors():
			log.Println(err.Error())

		case <-producer.Successes():
		case sample := <-ch:
			body, err := easyjson.Marshal(sample)
			Check(err)
			msg := sarama.ProducerMessage{
				Topic: cfg.Kafka.Topic,
				Key:   sarama.StringEncoder(sample.Tag),
				Value: sarama.ByteEncoder(body),
			}
			producer.Input() <- &msg
		case <-ctx.Done():
			return
		}
	}
}

func Run(args []string) {
	var cfg Config
	fs := flag.NewFlagSet("kafka", flag.ContinueOnError)
	fs.StringVar(&cfg.Kafka.Topic, "topic", "test", "Kafka topic")
	hosts := fs.String("hosts", "192.168.0.2:9092", "Comma-separated list of Kafka hosts")
	fs.StringVar(&cfg.Addr, "addr", "localhost:9876", "Serve address:port")
	err := fs.Parse(args)
	if err != nil {
		fmt.Print(err)
		os.Exit(2)
	}
	cfg.Kafka.Hosts = strings.Split(*hosts, ",")
	ch := make(chan model.Sample, 1000)
	ctx := context.Background()
	go saveLoop(ctx, ch, cfg)
	err = fasthttp.ListenAndServe(cfg.Addr, makeHandler(ch))
	Check(err)
}
