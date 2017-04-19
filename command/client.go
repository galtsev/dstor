package command

import (
	"dan/pimco"
	"dan/pimco/api"
	"dan/pimco/model"
	"flag"
	"fmt"
	"sync"
	"time"
)

func pwork(cfg pimco.ClientConfig, ch chan model.Sample) {
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
	fs := flag.NewFlagSet("client", flag.ExitOnError)
	concurrency := fs.Int("c", 1, "Concurrency")
	cfg := pimco.LoadConfigEx(fs, args...)
	fmt.Println(cfg)
	ch := make(chan model.Sample, 100)
	//work(cfg)
	var wg sync.WaitGroup
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func() {
			pwork(cfg.Client, ch)
			wg.Done()
		}()
	}
	gen := pimco.NewGenerator(cfg.Gen)
	cnt := 0
	t := time.Now()
	for gen.Next() {
		ch <- *gen.Sample()
		cnt++
		if cnt%20000 == 0 {
			fmt.Printf("%10d %10d\n", cnt, int(time.Since(t))/1000000)
			t = time.Now()
		}
	}
	close(ch)
	wg.Wait()
}
