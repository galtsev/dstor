package command

import (
	"flag"
	"github.com/galtsev/dstor"
	"github.com/galtsev/dstor/api"
	"github.com/galtsev/dstor/conf"
	"github.com/galtsev/dstor/model"
	"github.com/galtsev/dstor/util"
	"sync"
	"time"
)

func pwork(cfg conf.ClientConfig, ch chan model.Sample) {
	client := api.NewClient(cfg)
	currentBatchSize := 0
	for sample := range ch {
		client.Add(&sample)
		currentBatchSize++
		if currentBatchSize >= cfg.BatchSize {
			client.Flush()
			currentBatchSize = 0
		}
	}
	client.Close()
}

func Client(args []string) {
	cfg := conf.NewConfig()
	conf.Load(cfg)
	fs := flag.NewFlagSet("client", flag.ExitOnError)
	concurrency := fs.Int("c", 1, "Concurrency")
	fs.StringVar(&cfg.Client.Host, "host", "localhost:8787", "server host:port")
	rate := fs.Int("rate", 0, "request rate k*samples/sec, 0 for no limit")
	fs.Parse(args)
	ch := make(chan model.Sample, 1000)
	var wg sync.WaitGroup
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func() {
			pwork(cfg.Client, ch)
			wg.Done()
		}()
	}
	gen := pimco.NewGenerator(cfg.Gen)
	mediator := util.NewMediator(*rate*1000, time.Duration(50)*time.Millisecond)
	progress := util.NewProgress(100000)
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
			ch <- *gen.Sample()
			progress.Step()
		}
	}
	close(ch)
	wg.Wait()
}
