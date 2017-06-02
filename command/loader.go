package command

import (
	"fmt"
	"github.com/galtsev/dstor"
	"github.com/galtsev/dstor/api"
	. "github.com/galtsev/dstor/base"
	"github.com/galtsev/dstor/serializer"
	"github.com/galtsev/dstor/util"
	"math/rand"
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

type Stats struct {
	lock sync.Mutex
	b10  int
	b20  int
	b50  int
	b100 int
	bAll int
}

var (
	d10  = time.Duration(10) * time.Millisecond
	d20  = time.Duration(10) * time.Millisecond
	d50  = time.Duration(50) * time.Millisecond
	d100 = time.Duration(100) * time.Millisecond
)

func (s *Stats) Update(d time.Duration) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if d < d10 {
		s.b10++
	} else if d < d20 {
		s.b20++
	} else if d < d50 {
		s.b50++
	} else if d < d100 {
		s.b100++
	} else {
		s.bAll++
	}
}

func (s *Stats) String() string {
	return fmt.Sprintf("{10:%d; 20:%d; 50:%d; 100:%d; >100: %d}", s.b10, s.b20, s.b50, s.b100, s.bAll)
}

func reportLoop(client *api.Client, start, end time.Time, stats *Stats, ch chan string, wg *sync.WaitGroup) {
	for tag := range ch {
		startTime := time.Now()
		_ = client.Report(tag, start, end)
		stats.Update(time.Now().Sub(startTime))
	}
	wg.Done()
}

func Loader(args []string) {
	cfg := conf.NewConfig()
	conf.Load(cfg)
	fs := flag.NewFlagSet("loader", flag.ExitOnError)
	concurrency := fs.Int("c", 4, "per client goroutines")
	clients := fs.Int("t", 4, "clients")
	rate := fs.Int("rate", 0, "rate limit")
	fs.StringVar(&cfg.Client.Host, "host", "localhost:8787", "server host:port")
	reportStart := fs.String("report.start", "", "report start date YYYY-mm-dd HH:MM")
	reportDuration := fs.Duration("report.duration", time.Duration(24)*time.Hour, "report duration <int>{h|m|s}")

	conf.Parse(cfg, args, fs, "gen")

	gen := dstor.NewGenerator(cfg.Gen)
	progress := util.NewProgress(100000)
	url := fmt.Sprintf("http://%s/save", cfg.Client.Host)
	ch := make(chan model.Sample, 1000)
	var wg sync.WaitGroup

	// starting actual loaders
	for cn := 0; cn < *clients; cn++ {
		client := &fasthttp.PipelineClient{Addr: cfg.Client.Host}
		wg.Add(*concurrency)
		for i := 0; i < *concurrency; i++ {
			go sendLoop(client, url, ch, &wg)
		}
	}

	// starting reporters if requested
	var stats Stats
	reportCh := make(chan string, 10)
	finishCh := make(chan struct{})
	if *reportStart != "" {
		start, err := time.Parse(DATE_FORMAT, *reportStart)
		Check(err)
		end := start.Add(*reportDuration)
		nreporters := 10
		client := api.NewClient(cfg.Client)
		wg.Add(nreporters)
		for i := 0; i < nreporters; i++ {
			go reportLoop(client, start, end, &stats, reportCh, &wg)
		}
		// report scheduler
		var tags []string
		for i := 0; i < cfg.Gen.Tags; i++ {
			tags = append(tags, fmt.Sprintf("tag%d", i))
		}
		go func() {
			ticker := time.Tick(time.Second)
			for {
				select {
				case <-finishCh:
					close(reportCh)
					return
				case <-ticker:
					for i := 0; i < nreporters; i++ {
						reportCh <- tags[rand.Intn(len(tags))]
					}

				}
			}
		}()
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
	close(finishCh)
	close(ch)
	wg.Wait()
	fmt.Println(&stats)
}
