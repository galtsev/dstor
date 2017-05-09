package command

import (
	. "dan/pimco/base"
	"dan/pimco/conf"
	"dan/pimco/injector"
	"dan/pimco/ldb"
	"dan/pimco/prom"
	"dan/pimco/server"
	"dan/pimco/zoo"
	"github.com/valyala/fasthttp"
)

func Proxy(args []string) {
	var cfg conf.Config
	conf.Load(&cfg)

	storage := injector.MakeStorage("kafka", cfg, nil)

	zk := zoo.New(cfg.Zookeeper.Servers)
	defer zk.Close()
	zk.WatchReporters()

	reporter := ldb.NewReporter(cfg, zk)
	srv := server.NewServer(cfg.Server, storage, reporter)
	// serve metrics
	prom.Setup(cfg.Metrics)

	// TODO - handle graceful shutdown - drain save channels first
	Check(fasthttp.ListenAndServe(cfg.Server.Addr, srv.Route))

}
