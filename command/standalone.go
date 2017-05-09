package command

import (
	. "dan/pimco/base"
	"dan/pimco/conf"
	"dan/pimco/injector"
	"dan/pimco/prom"
	"dan/pimco/server"
	"flag"
	"github.com/valyala/fasthttp"
)

func Standalone(args []string) {
	var cfg conf.Config
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
