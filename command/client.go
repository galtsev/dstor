package command

import (
	"dan/pimco"
	"dan/pimco/api"
	"dan/pimco/model"
	"dan/pimco/util"
	"flag"
	"fmt"
	"strings"
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

type pstep struct {
	d time.Duration // step duration
	r int           // rate, samples per second
}

func parseSteps(src string) []pstep {
	var res []pstep
	for _, part := range strings.Split(src, " ") {
		var rate, duration int
		fmt.Sscanf(part, "%d:%d", &rate, &duration)
		res = append(res, pstep{r: rate * 1000, d: time.Duration(duration) * time.Second})
	}
	return res
}

func Client(args []string) {
	fs := flag.NewFlagSet("client", flag.ExitOnError)
	concurrency := fs.Int("c", 1, "Concurrency")
	pattern := fs.String("pattern", "10:1", "load pattern, list of steps in form <rate k*samples/second>:<step duration seconds>, separated by spaces")
	cfg := pimco.LoadConfigEx(fs, args...)
	fmt.Println(cfg)
	ch := make(chan model.Sample, 100)
	mediator := make(chan int, 100)
	steps := parseSteps(*pattern)
	// run mediator
	go func() {
		for {
			for _, step := range steps {
				util.Mediate(mediator, step.r, time.Duration(50)*time.Millisecond, &step.d)
			}
		}
	}()
	var wg sync.WaitGroup
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func() {
			pwork(cfg.Client, ch)
			wg.Done()
		}()
	}
	gen := pimco.NewGenerator(cfg.Gen)
	progress := util.NewProgress(100000)
	for {
		n := <-mediator
		for i := 0; i < n; i++ {
			gen.Next()
			ch <- *gen.Sample()
			progress.Step()
		}
	}
	close(ch)
	wg.Wait()
}
