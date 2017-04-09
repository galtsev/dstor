package main

import (
	. "dan/base"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/valyala/fasthttp"
	"math/rand"
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

type Sample struct {
	Tag    string
	Values []float64
	TS     int64 //timestamp in nanoseconds
}

func init() {
	for i := 0; i < numTags; i++ {
		tags = append(tags, fmt.Sprintf("tag_%d", i))
	}
}

func work(count, step int, ts int64, wg *sync.WaitGroup) {
	var values [10]float64
	var sample Sample
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
		body, err := json.Marshal(sample)
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
	flag.Parse()
	dt, err := time.Parse(date_format, *startTime)
	Check(err)
	ts := dt.UnixNano()
	wg.Add(*concurrency)
	for i := 0; i < *concurrency; i++ {
		go work(*count, *step, ts+int64(i*2), &wg)
	}
	wg.Wait()
}
