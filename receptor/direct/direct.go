package direct

import (
	"context"
	"dan/pimco/model"
	. "dan/pimco/util"
	"github.com/mailru/easyjson"

	"flag"
	"fmt"
	"github.com/influxdata/influxdb/client/v2"
	"github.com/valyala/fasthttp"
	_ "log"
	"os"
	"time"
)

const (
	database = "test"
)

var (
	FIELD_NAMES = []string{"V1", "V2", "V3", "V4", "V5", "V6", "V7", "V8", "V9", "V10"}
	EMPTY_TAGS  = map[string]string{}
)

type Config struct {
	InfluxURL string
	DB        string
	Addr      string
}

func makeHandler(ch chan model.Sample) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		//log.Printf("new request")
		var samples model.Samples
		err := easyjson.Unmarshal(ctx.PostBody(), &samples)
		if err != nil {
			ctx.Error(err.Error(), fasthttp.StatusBadRequest)
			return
		}
		//log.Printf("Queued: tag: %s, values: %v", sample.Tag, sample.Values)
		for _, sample := range samples {
			ch <- sample
		}
		ctx.SetStatusCode(fasthttp.StatusNoContent)
	}
}

func addSample(sample *model.Sample, batch client.BatchPoints) {
	fields := make(map[string]interface{})
	for idx, fn := range FIELD_NAMES {
		fields[fn] = sample.Values[idx]
	}
	point, _ := client.NewPoint(sample.Tag, EMPTY_TAGS, fields, time.Unix(0, sample.TS))
	batch.AddPoint(point)
}

func saveLoop(ctx context.Context, ch chan model.Sample, cfg Config) {
	conn, err := client.NewHTTPClient(client.HTTPConfig{Addr: cfg.InfluxURL})
	Check(err)
	defer conn.Close()
	batchPointsConfig := client.BatchPointsConfig{
		Database: cfg.DB,
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
		//log.Printf("Written batch with %d records", len(batch.Points()))
	}
}

func Run(args []string) {
	var cfg Config
	fs := flag.NewFlagSet("client", flag.ContinueOnError)
	fs.StringVar(&cfg.InfluxURL, "influx", "http://localhost:8086", "Influxdb url")
	fs.StringVar(&cfg.DB, "db", "test", "database name")
	fs.StringVar(&cfg.Addr, "addr", "localhost:9876", "Serve address:port")
	err := fs.Parse(args)
	if err != nil {
		fmt.Print(err)
		os.Exit(2)
	}

	ch := make(chan model.Sample, 1000)
	ctx := context.Background()
	go saveLoop(ctx, ch, cfg)
	err = fasthttp.ListenAndServe(cfg.Addr, makeHandler(ch))
	Check(err)
}
