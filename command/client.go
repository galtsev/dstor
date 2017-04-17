package command

import (
	"dan/pimco"
	. "dan/pimco/base"
	"dan/pimco/model"
	"dan/pimco/serializer"
	"errors"
	"fmt"
	"github.com/valyala/fasthttp"
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
	if len(w.batch) == 0 {
		return
	}
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
	w.batch = w.batch[:0]
}

func (w *clientWriter) Close() {
	w.Flush()
}

func work(cfg pimco.Config) {
	//kafkaWriter := kafka.NewWriter(cfg.Kafka, int32(0))
	writer := clientWriter{
		url: "http://" + cfg.ReceptorServer.Addr,
		szr: serializer.NewSerializer("easyjson"),
	}
	gen := pimco.NewGenerator(cfg.Gen)
	currentBatchSize := 0
	for gen.Next() {
		writer.Add(gen.Sample())
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
	work(cfg)
}
