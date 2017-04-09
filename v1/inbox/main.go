package main

import (
	"context"
	. "dan/base"
	"encoding/json"
	"github.com/influxdata/influxdb/client/v2"
	"github.com/valyala/fasthttp"
	"log"
	"time"
)

const (
	database = "test"
)

type Sample struct {
	Tag    string
	Values []float64
	TS     int64 //timestamp in nanoseconds
}

var (
	FIELD_NAMES = []string{"V1", "V2", "V3", "V4", "V5", "V6", "V7", "V8", "V9", "V10"}
	EMPTY_TAGS  = map[string]string{}
)

func makeHandler(ch chan Sample) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		//log.Printf("new request")
		var sample Sample
		err := json.Unmarshal(ctx.PostBody(), &sample)
		if err != nil {
			ctx.Error(err.Error(), fasthttp.StatusBadRequest)
			return
		}
		if len(sample.Values) < len(FIELD_NAMES) {
			ctx.Error("Expected 10 values", fasthttp.StatusBadRequest)
			return
		}
		//log.Printf("Queued: tag: %s, values: %v", sample.Tag, sample.Values)
		ch <- sample
		ctx.SetStatusCode(fasthttp.StatusNoContent)
	}
}

func addSample(sample *Sample, batch client.BatchPoints) {
	fields := make(map[string]interface{})
	for idx, fn := range FIELD_NAMES {
		fields[fn] = sample.Values[idx]
	}
	point, _ := client.NewPoint(sample.Tag, EMPTY_TAGS, fields, time.Unix(0, sample.TS))
	batch.AddPoint(point)
}

func saveLoop(ctx context.Context, ch chan Sample) {
	conn, err := client.NewHTTPClient(client.HTTPConfig{Addr: "http://localhost:8086"})
	Check(err)
	defer conn.Close()
	batchPointsConfig := client.BatchPointsConfig{
		Database: database,
	}
loop:
	for {
		batch, err := client.NewBatchPoints(batchPointsConfig)
		Check(err)
		// collect at least one sample from cannel + as much as we can without blocking
		select {
		case sample := <-ch:
			addSample(&sample, batch)
		case <-ctx.Done():
			break loop
		}
		more := true
		cnt := 0
		for more && cnt < 1000 {
			select {
			case sample := <-ch:
				addSample(&sample, batch)
				cnt++
			default:
				more = false
			}
		}
		conn.Write(batch)
		log.Printf("Written batch with %d records", len(batch.Points()))
	}
}

func main() {
	ch := make(chan Sample, 1000)
	ctx := context.Background()
	go saveLoop(ctx, ch)
	err := fasthttp.ListenAndServe("localhost:9876", makeHandler(ch))
	Check(err)
}
