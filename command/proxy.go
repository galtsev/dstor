package command

import (
	. "dan/pimco/base"
	"dan/pimco/conf"
	"dan/pimco/injector"
	"dan/pimco/prom"
	"dan/pimco/server"
	"github.com/valyala/fasthttp"
)

func Proxy(args []string) {
	var cfg conf.Config
	conf.Load(&cfg, args...)

	storage := injector.MakeStorage("kafka", cfg, nil)
	reporter := injector.MakeReporter("remote", cfg)
	srv := server.NewServer(cfg.Server, storage, reporter)
	// serve metrics
	prom.Setup(cfg.Metrics)

	// TODO - handle graceful shutdown - drain save channels first
	Check(fasthttp.ListenAndServe(cfg.Server.Addr, srv.Route))

}
