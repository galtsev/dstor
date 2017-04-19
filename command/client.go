package command

import (
	"dan/pimco"
	"dan/pimco/api"
	"fmt"
	"time"
)

func work(cfg pimco.Config) {
	client := api.NewClient(cfg.Client)
	gen := pimco.NewGenerator(cfg.Gen)
	cnt := 0
	currentBatchSize := 0
	t := time.Now()
	for gen.Next() {
		client.Add(gen.Sample())
		currentBatchSize++
		if currentBatchSize >= cfg.Client.BatchSize {
			client.Flush()
			currentBatchSize = 0
		}
		if cnt%10000 == 0 {
			fmt.Printf("%10d %10d\n", cnt, int(time.Since(t))/1000000)
			t = time.Now()
		}
		cnt++
	}
	client.Close()
}

func Client(args []string) {
	cfg := pimco.LoadConfig(args...)
	fmt.Println(cfg)
	work(cfg)
}
