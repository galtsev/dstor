package command

import (
	"dan/pimco"
	. "dan/pimco/base"
	"dan/pimco/kafka"
	"dan/pimco/model"
	"dan/pimco/serializer"
	"fmt"
	"github.com/valyala/fasthttp"
	"time"
)

func kafkaSaveLoop(cfg pimco.KafkaConfig, partition int32, ch chan model.Sample) {
	kafkaWriter := kafka.NewWriter(cfg, partition)
	w := pimco.NewWriter(kafkaWriter, cfg.BatchSize, time.Duration(cfg.FlushDelay)*time.Millisecond)
	for sample := range ch {
		w.Write(&sample)
	}
	w.Close()
}

func makeHandler(channels []chan model.Sample) fasthttp.RequestHandler {
	szr := serializer.NewSerializer("easyjson")
	partitionFor := pimco.MakePartitioner(len(channels))
	return func(ctx *fasthttp.RequestCtx) {
		var samples model.Samples
		err := szr.Unmarshal(ctx.PostBody(), &samples)
		if err != nil {
			ctx.Error(err.Error(), fasthttp.StatusBadRequest)
			return
		}
		for _, sample := range samples {
			p := partitionFor(sample.Tag)
			channels[p] <- sample
		}
		ctx.SetStatusCode(fasthttp.StatusNoContent)
	}
}

func Recept2Kafka(args []string) {
	cfg := pimco.LoadConfig(args...)
	fmt.Println(cfg)
	// receptor operate with all partitions
	saveChannels := make([]chan model.Sample, cfg.Kafka.NumPartitions)
	for partition := range saveChannels {
		ch := make(chan model.Sample, 1000)
		go kafkaSaveLoop(cfg.Kafka, int32(partition), ch)
		saveChannels[partition] = ch
	}
	err := fasthttp.ListenAndServe(cfg.ReceptorServer.Addr, makeHandler(saveChannels))
	// TODO - handle graceful shutdown - drain save channels first
	Check(err)

}
