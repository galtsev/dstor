package command

import (
	. "dan/pimco/base"
	"dan/pimco/conf"
	"dan/pimco/injector"
	"dan/pimco/kafka"
	"dan/pimco/prom"
	"dan/pimco/server"
	"dan/pimco/zoo"
	"flag"
	"github.com/valyala/fasthttp"
	"log"
	"sync"
)

func StorageNode(args []string) {
	var cfg conf.Config = *conf.NewConfig()
	var wg sync.WaitGroup
	conf.Load(&cfg)
	fs := flag.NewFlagSet("storagenode", flag.ExitOnError)
	backendName := fs.String("backend", "leveldb", "Backend storage to use")
	fs.Parse(args)

	// here we have circular dependency OffsetStorage->NodeId->Backend->OffsetStorage
	// OffsetStorage use nodeId to identify backend in external system (kafka)
	// so, nodeId need to be stored with backend and requested from instantiated backend
	// but we need OffsetStorage to instantiate backend
	// to solve this, we make NodeId a lazy dependency of OffsetStorage
	offsetStorage := kafka.NewOffsetStorage(cfg.Kafka)
	backend := injector.MakeBackend(*backendName, cfg, offsetStorage)
	defer backend.Close()

	offsetStorage.SetNodeId(injector.NodeId(backend))

	partitions := cfg.Server.ConsumePartitions
	wg.Add(len(partitions))
	for _, partition := range partitions {
		go kafka.PartitionLoader(cfg.Kafka, partition, offsetStorage.GetOffset(partition), backend, &wg)
	}

	// strage node don't accept samples through http, so storage is nil
	srv := server.NewServer(cfg.Server, nil, backend)
	// serve metrics
	prom.Setup(cfg.Metrics)

	// register itself as reporter as soon as all partition consumers come close enough to HighWaterMark
	go func() {
		wg.Wait()
		zk := zoo.New(cfg.Zookeeper.Servers)
		defer zk.Close()
		zk.Register(cfg.Server.AdvertizeHost, partitions)
		log.Printf("Registered as reporter for partitions %v", partitions)

	}()

	// TODO - handle graceful shutdown - drain save channels first
	Check(fasthttp.ListenAndServe(cfg.Server.Addr, srv.Route))

}
