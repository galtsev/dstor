package command

import (
	. "github.com/galtsev/dstor/base"
	"github.com/galtsev/dstor/conf"
	"github.com/galtsev/dstor/injector"
	"github.com/galtsev/dstor/ldb"
	"github.com/galtsev/dstor/prom"
	"github.com/galtsev/dstor/server"
	"github.com/galtsev/dstor/zoo"
	"github.com/valyala/fasthttp"
)

func Proxy(args []string) {
	cfg := *conf.NewConfig()
	conf.Load(&cfg)

	storage := injector.MakeStorage("kafka", cfg, nil)

	zk := zoo.New(cfg.Zookeeper.Servers)
	defer zk.Close()
	go zk.WatchReporters()

	reporter := ldb.NewReporter(cfg, zk)
	srv := server.NewServer(cfg.Server, storage, reporter)
	// serve metrics
	prom.Setup(cfg.Metrics)

	// TODO - handle graceful shutdown - drain save channels first
	Check(fasthttp.ListenAndServe(cfg.Server.Addr, srv.Route))

}
