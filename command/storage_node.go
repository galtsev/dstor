package command

import (
	. "dan/pimco/base"
	"dan/pimco/conf"
	"dan/pimco/injector"
	"dan/pimco/kafka"
	"dan/pimco/prom"
	"dan/pimco/server"
	"flag"
	"fmt"
	"github.com/valyala/fasthttp"
)

func StorageNode(args []string) {
	var cfg conf.Config
	conf.Load(&cfg, args...)
	fs := flag.NewFlagSet("storagenode", flag.ExitOnError)
	backendName := fs.String("backend", "leveldb", "Backend storage to use")
	nodeId := fs.String("nodeId", "", "node id")
	fs.Parse(args)

	if *nodeId == "" {
		panic(fmt.Errorf("Missing node id"))
	}

	offsetStorage := kafka.NewOffsetStorage(*nodeId, cfg.Kafka)
	backend := injector.MakeBackend(*backendName, cfg, offsetStorage)

	for _, p := range cfg.Server.ConsumePartitions {
		go kafka.PartitionLoader(cfg.Kafka, int32(p), backend)
	}

	// strage node don't accept samples through http, so storage is nil
	srv := server.NewServer(cfg.Server, nil, backend)
	// serve metrics
	prom.Setup(cfg.Metrics)

	// TODO - handle graceful shutdown - drain save channels first
	Check(fasthttp.ListenAndServe(cfg.Server.Addr, srv.Route))

}
