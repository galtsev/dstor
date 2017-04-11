package main

import (
	"context"
	. "dan/base"
	"dan/pimco/v1/model"
	"github.com/Shopify/sarama"
	"github.com/mailru/easyjson"
	"github.com/valyala/fasthttp"
	"log"
	"os"
	_ "time"
)

var (
	FIELD_NAMES = []string{"V1", "V2", "V3", "V4", "V5", "V6", "V7", "V8", "V9", "V10"}
	EMPTY_TAGS  = map[string]string{}
)

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

func saveLoop(ctx context.Context, ch chan model.Sample) {
	conf := sarama.NewConfig()
	kafkaHost := os.Getenv("KAFKA_HOST")
	kafkaTopic := os.Getenv("KAFKA_TOPIC")
	producer, err := sarama.NewAsyncProducer([]string{kafkaHost}, conf)
	Check(err)
	log.Println("Producer started")
	for {
		select {
		case err := <-producer.Errors():
			log.Println(err.Error())

		case <-producer.Successes():
			log.Println("ok")
		case sample := <-ch:
			body, err := easyjson.Marshal(sample)
			Check(err)
			msg := sarama.ProducerMessage{
				Topic: kafkaTopic,
				Key:   sarama.StringEncoder(sample.Tag),
				Value: sarama.ByteEncoder(body),
			}
			producer.Input() <- &msg
		case <-ctx.Done():
			return
		}
	}
}

func saveLoop2(ctx context.Context, ch chan model.Sample) {
	for _ = range ch {
	}
}

func main() {
	ch := make(chan model.Sample, 1000)
	ctx := context.Background()
	go saveLoop(ctx, ch)
	err := fasthttp.ListenAndServe("localhost:9876", makeHandler(ch))
	Check(err)
}
