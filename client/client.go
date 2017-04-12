package client

import (
	. "dan/pimco/util"
	"dan/pimco/v1/model"
	"errors"
	"flag"
	"fmt"
	"github.com/mailru/easyjson"
	"github.com/valyala/fasthttp"
	"math/rand"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"time"
)

const (
	numTags = 2
	// Mon Jan 2 15:04:05 -0700 MST 2006
	date_format = "2006-01-02 15:04:05"
	url         = "http://localhost:9876"
)

var (
	tags []string
)

func init() {
	for i := 0; i < numTags; i++ {
		tags = append(tags, fmt.Sprintf("tag_%d", i))
	}
}

func mkSample(ts int64) model.Sample {
	values := make([]float64, 10)
	v0 := float64(ts / 1000000000)
	for t := range values {
		values[t] = v0 * float64(t+1)
	}
	return model.Sample{
		Tag:    tags[rand.Intn(len(tags))],
		Values: values,
		TS:     ts,
	}
}

func work(count, bs int, ts, step int64, wg *sync.WaitGroup) {
	samples := make([]model.Sample, bs)
	for i := 0; i < count/bs; i++ {
		samples = samples[:0]
		for j := 0; j < bs; j++ {
			s := mkSample(ts)
			samples = append(samples, s)
			ts += step
		}
		req := fasthttp.AcquireRequest()
		resp := fasthttp.AcquireResponse()
		req.Header.SetMethod("POST")
		req.SetRequestURI(url)
		body, err := easyjson.Marshal(model.Samples(samples))
		Check(err)
		req.SetBody(body)
		Check(fasthttp.Do(req, resp))
		if resp.StatusCode() >= 300 {
			panic(errors.New(fmt.Sprintf("Bad response: %d", resp.StatusCode())))
		}
		fasthttp.ReleaseRequest(req)
		fasthttp.ReleaseResponse(resp)
	}
	wg.Done()
}

func Run(args []string) {
	var wg sync.WaitGroup
	fs := flag.NewFlagSet("client", flag.ContinueOnError)
	startTime := fs.String("start", "2017-04-08 09:00:00", "Start date")
	step := fs.Int("step", 300, "Step, ms")
	count := fs.Int("count", 1, "count")
	bs := fs.Int("bs", 1, "batch size")
	concurrency := fs.Int("c", 1, "concurrency")
	cpuprofile := fs.String("cpuprofile", "", "write cpu profile to <file>")
	memprofile := fs.String("memprofile", "", "write memory profile to <file>")
	err := fs.Parse(args)
	if err != nil {
		fmt.Print(err)
		os.Exit(2)
	}
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		Check(err)
		Check(pprof.StartCPUProfile(f))
		defer pprof.StopCPUProfile()
	}
	dt, err := time.Parse(date_format, *startTime)
	Check(err)
	ts := dt.UnixNano()
	wg.Add(*concurrency)
	for i := 0; i < *concurrency; i++ {
		go work(*count/(*concurrency), *bs, int64(*step*1000000), ts+int64(i*2), &wg)
	}
	wg.Wait()
	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		Check(err)
		runtime.GC()
		Check(pprof.WriteHeapProfile(f))
		f.Close()
	}

}
