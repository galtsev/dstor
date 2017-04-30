package command

import (
	"dan/pimco"
	. "dan/pimco/base"
	"dan/pimco/conf"
	"dan/pimco/kafka"
	"dan/pimco/model"
	"dan/pimco/serializer"
	"fmt"
	"github.com/valyala/fasthttp"
)

func kafkaSaveLoop(cfg conf.Config, partition int32, ch chan model.Sample) {
	w := kafka.NewWriter(cfg.Kafka, partition)
	for sample := range ch {
		w.AddSample(&sample)
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
	cfg := conf.LoadConfig(args...)
	fmt.Println(cfg)
	// receptor operate with all partitions
	saveChannels := make([]chan model.Sample, cfg.Kafka.NumPartitions)
	for partition := range saveChannels {
		ch := make(chan model.Sample, 1000)
		go kafkaSaveLoop(cfg, int32(partition), ch)
		saveChannels[partition] = ch
	}
	err := fasthttp.ListenAndServe(cfg.Server.Addr, makeHandler(saveChannels))
	// TODO - handle graceful shutdown - drain save channels first
	Check(err)

}
