package command

import (
	. "dan/pimco/base"
	"dan/pimco/conf"
	"dan/pimco/injector"
	"dan/pimco/kafka"
	"dan/pimco/prom"
	"dan/pimco/server"
	"flag"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/valyala/fasthttp"
	"log"
	"net/http"
)

func Serve(args []string) {
	fs := flag.NewFlagSet("server", flag.ExitOnError)
	cfg := conf.LoadConfigEx(fs, args...)
	log.Println(cfg)
	inj := injector.New(cfg)
	srv := server.NewServer(cfg.Server, inj.Storage(), inj.Reporter())
	for _, p := range cfg.Server.ConsumePartitions {
		go kafka.PartitionLoader(cfg.Kafka, int32(p), inj.Storage())
	}
	// serve metrics
	prom.Setup(cfg.Metrics)
	http.Handle("/metrics", promhttp.Handler())
	go func() {
		log.Fatalln(http.ListenAndServe(cfg.Metrics.Addr, nil))
	}()
	err := fasthttp.ListenAndServe(cfg.Server.Addr, srv.Route)
	// TODO - handle graceful shutdown - drain save channels first
	Check(err)

}
