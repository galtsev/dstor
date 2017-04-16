package temp

import (
	"dan/pimco"
	. "dan/pimco/base"
	"dan/pimco/model"
	"dan/pimco/serializer"
	"errors"
	"fmt"
	"github.com/valyala/fasthttp"
	"time"
)

type clientWriter struct {
	batch []model.Sample
	url   string
	szr   serializer.Serializer
}

func (w *clientWriter) Add(sample *model.Sample) {
	w.batch = append(w.batch, *sample)
}

func (w *clientWriter) Flush() {
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	req.Header.SetMethod("POST")
	req.SetRequestURI(w.url)
	req.SetBody(w.szr.Marshal(model.Samples(w.batch)))
	Check(fasthttp.Do(req, resp))
	if resp.StatusCode() >= 300 {
		msg := fmt.Sprintf("Bad response: %d", resp.StatusCode())
		panic(errors.New(msg))
	}
	fasthttp.ReleaseRequest(req)
	fasthttp.ReleaseResponse(resp)
}

func (w *clientWriter) Close() {
	w.Flush()
}

func work(cfg pimco.Config, count int) {
	//kafkaWriter := kafka.NewWriter(cfg.Kafka, int32(0))
	writer := clientWriter{
		url: "http://" + cfg.HTTPServer.Addr,
		szr: serializer.NewSerializer("easyjson"),
	}
	dt, err := time.Parse(date_format, cfg.Gen.Start)
	Check(err)
	gen := pimco.NewGenerator(cfg.Gen.Tags, dt.UnixNano(), cfg.Gen.Step)
	currentBatchSize := 0
	for i := 0; i < count; i++ {
		writer.Add(gen.Next())
		currentBatchSize++
		if currentBatchSize >= cfg.Client.BatchSize {
			writer.Flush()
			currentBatchSize = 0
		}
	}
	writer.Close()
}

func Client(args []string) {
	cfg := pimco.LoadConfig(args...)
	fmt.Println(cfg)
	work(cfg, cfg.Gen.Count)
}
