package command

import (
	"fmt"
	"github.com/galtsev/dstor"
	. "github.com/galtsev/dstor/base"
	"github.com/galtsev/dstor/serializer"
	"github.com/galtsev/dstor/util"
	"sync"
	"time"

	"flag"
	"github.com/galtsev/dstor/conf"
	"github.com/galtsev/dstor/model"
	"github.com/valyala/fasthttp"
)

type HTTPClient interface {
	Do(req *fasthttp.Request, resp *fasthttp.Response) error
}

func sendLoop(client HTTPClient, url string, ch chan model.Sample, wg *sync.WaitGroup) {
	defer wg.Done()
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)
	req.Header.SetMethod("POST")
	req.SetRequestURI(url)
	szr := serializer.EasyJsonSerializer{}
	for sample := range ch {
		body := szr.Marshal(sample)
		req.SetBody(body)
		Check(client.Do(req, resp))
		if resp.StatusCode() >= 300 {
			panic(fmt.Errorf("Bad response: %d", resp.StatusCode()))
		}
	}
}

func Loader(args []string) {
	cfg := conf.NewConfig()
	conf.Load(cfg)
	fs := flag.NewFlagSet("loader", flag.ExitOnError)
	concurrency := fs.Int("c", 4, "Concurrency")
	clients := fs.Int("t", 4, "clients")
	rate := fs.Int("rate", 0, "rate limit")
	fs.StringVar(&cfg.Client.Host, "host", "localhost:8787", "server host:port")
	fs.IntVar(&cfg.Gen.Count, "n", cfg.Gen.Count, "Num samples")
	fs.Parse(args)

	gen := dstor.NewGenerator(cfg.Gen)
	progress := util.NewProgress(100000)
	url := fmt.Sprintf("http://%s/save", cfg.Client.Host)
	ch := make(chan model.Sample, 1000)
	var wg sync.WaitGroup
	for cn := 0; cn < *clients; cn++ {
		client := &fasthttp.PipelineClient{Addr: cfg.Client.Host}
		wg.Add(*concurrency)
		for i := 0; i < *concurrency; i++ {
			go sendLoop(client, url, ch, &wg)
		}
	}
	if *rate == 0 {
		for gen.Next() {
			ch <- *gen.Sample()
			progress.Step()
		}
	} else {
		proceed := true
		mediator := util.NewMediator(*rate, time.Duration(100)*time.Millisecond)
		for proceed {
			n := mediator.Next()
			for i := 0; i < n; i++ {
				if gen.Next() {
					ch <- *gen.Sample()
					progress.Step()
				} else {
					proceed = false
					break
				}
			}
		}
	}
	close(ch)
	wg.Wait()
}
