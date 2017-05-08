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
)

func StorageNode(args []string) {
	var cfg conf.Config = *conf.NewConfig()
	conf.Load(&cfg, args...)
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

	nodeId := injector.NodeId(backend)
	offsetStorage.SetNodeId(nodeId)

	for _, p := range cfg.Server.ConsumePartitions {
		go kafka.PartitionLoader(cfg.Kafka, int32(p), offsetStorage.GetOffset(int32(p)), backend)
	}

	// strage node don't accept samples through http, so storage is nil
	srv := server.NewServer(cfg.Server, nil, backend)
	// serve metrics
	prom.Setup(cfg.Metrics)

	zk := zoo.New(cfg.Zookeeper.Servers, nodeId)
	zk.Register(cfg.Server.AdvertizeHost, cfg.Server.ConsumePartitions)
	defer zk.Close()

	// TODO - handle graceful shutdown - drain save channels first
	Check(fasthttp.ListenAndServe(cfg.Server.Addr, srv.Route))

}
