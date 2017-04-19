package command

import (
	"dan/pimco"
	"dan/pimco/api"
	"fmt"
)

func work(cfg pimco.Config) {
	client := api.NewClient(cfg.Client)
	gen := pimco.NewGenerator(cfg.Gen)
	currentBatchSize := 0
	for gen.Next() {
		client.Add(gen.Sample())
		currentBatchSize++
		if currentBatchSize >= cfg.Client.BatchSize {
			client.Flush()
			currentBatchSize = 0
		}
	}
	client.Close()
}

func Client(args []string) {
	cfg := pimco.LoadConfig(args...)
	fmt.Println(cfg)
	work(cfg)
}
