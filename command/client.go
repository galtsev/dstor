package command

import (
	"flag"
	"github.com/galtsev/dstor"
	"github.com/galtsev/dstor/api"
	"github.com/galtsev/dstor/conf"
	"github.com/galtsev/dstor/util"
	"time"
)

func Client(args []string) {
	cfg := conf.NewConfig()
	conf.Load(cfg)
	fs := flag.NewFlagSet("client", flag.ExitOnError)
	fs.IntVar(&cfg.Client.Concurrency, "c", 4, "Concurrency")
	fs.StringVar(&cfg.Client.Host, "host", "localhost:8787", "server host:port")
	rate := fs.Int("rate", 0, "request rate k*samples/sec, 0 for no limit")
	fs.Parse(args)
	gen := dstor.NewGenerator(cfg.Gen)
	mediator := util.NewMediator(*rate*1000, time.Duration(50)*time.Millisecond)
	progress := util.NewProgress(100000)
	client := api.NewClient(cfg.Client)
	var proceed bool = true
	var n int = 1000
	for proceed {
		if *rate != 0 {
			n = mediator.Next()
		}
		for i := 0; i < n; i++ {
			proceed = gen.Next()
			if !proceed {
				break
			}
			client.AddSample(gen.Sample())
			progress.Step()
		}
	}
	client.Close()
}
