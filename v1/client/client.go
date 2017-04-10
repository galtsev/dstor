package main

import (
	. "dan/base"
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

func work(count, step int, ts int64, wg *sync.WaitGroup) {
	var values [10]float64
	var sample model.Sample
	for i := 0; i < count; i++ {
		sample.Tag = tags[rand.Intn(len(tags))]
		v0 := float64(ts / 1000000000)
		for t := range values {
			values[t] = v0 * float64(t+1)
		}
		sample.Values = values[0:10]
		sample.TS = ts
		req := fasthttp.AcquireRequest()
		resp := fasthttp.AcquireResponse()
		req.Header.SetMethod("POST")
		req.SetRequestURI(url)
		body, err := easyjson.Marshal(sample)
		Check(err)
		req.SetBody(body)
		Check(fasthttp.Do(req, resp))
		if resp.StatusCode() >= 300 {
			panic(errors.New(fmt.Sprintf("Bad response: %d", resp.StatusCode())))
		}
		fasthttp.ReleaseRequest(req)
		fasthttp.ReleaseResponse(resp)
		ts += int64(step * 1000000)
	}
	wg.Done()
}

func main() {
	var wg sync.WaitGroup
	startTime := flag.String("start", "2017-04-08 09:00:00", "Start date")
	step := flag.Int("step", 300, "Step, ms")
	count := flag.Int("count", 1, "count")
	concurrency := flag.Int("c", 1, "concurrency")
	cpuprofile := flag.String("cpuprofile", "", "write cpu profile to <file>")
	memprofile := flag.String("memprofile", "", "write memory profile to <file>")
	flag.Parse()
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
		go work(*count/(*concurrency), *step, ts+int64(i*2), &wg)
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
