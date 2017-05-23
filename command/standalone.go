package command

import (
	"flag"
	. "github.com/galtsev/dstor/base"
	"github.com/galtsev/dstor/conf"
	"github.com/galtsev/dstor/injector"
	"github.com/galtsev/dstor/prom"
	"github.com/galtsev/dstor/server"
	"github.com/valyala/fasthttp"
)

func Standalone(args []string) {
	cfg := *conf.NewConfig()
	conf.Load(&cfg)
	fs := flag.NewFlagSet("standalone", flag.ExitOnError)
	backendName := fs.String("backend", "leveldb", "Backend to use")
	fs.Parse(args)

	backend := injector.MakeBackend(*backendName, cfg, nil)
	srv := server.NewServer(cfg.Server, backend, backend)
	// serve metrics
	prom.Setup(cfg.Metrics)

	// TODO - handle graceful shutdown - drain save channels first
	Check(fasthttp.ListenAndServe(cfg.Server.Addr, srv.Route))

}
