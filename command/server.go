package command

import (
	"dan/pimco"
	. "dan/pimco/base"
	"dan/pimco/model"
	"dan/pimco/phttp"
	"dan/pimco/prom"
	"dan/pimco/serializer"
	"encoding/json"
	"flag"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/valyala/fasthttp"
	"log"
	"net/http"
	"time"
)

type Server struct {
	backend     pimco.Backend
	json        serializer.Serializer
	partitioner func(string) int32
}

func NewServer(cfg pimco.Config) *Server {
	server := Server{
		json:        serializer.NewSerializer("easyjson"),
		partitioner: pimco.MakePartitioner(cfg.Kafka.NumPartitions),
		backend:     pimco.MakeBackend(cfg.Server.Backend, cfg),
	}
	return &server
}

func (srv *Server) Route(ctx *fasthttp.RequestCtx) {
	start := time.Now()
	var path string
	switch string(ctx.Path()) {
	case "/write":
		srv.handleWrite(ctx)
		path = "write"
	case "/report":
		srv.handleReport(ctx)
		path = "report"
	default:
		ctx.NotFound()
	}
	if path != "" {
		prom.RequestTime(path, time.Now().Sub(start))
	}
}

func (srv *Server) handleWrite(ctx *fasthttp.RequestCtx) {
	var samples model.Samples
	err := srv.json.Unmarshal(ctx.PostBody(), &samples)
	if err != nil {
		ctx.Error(err.Error(), fasthttp.StatusBadRequest)
		return
	}
	for _, sample := range samples {
		srv.backend.AddSample(&sample)
	}
	ctx.SetStatusCode(fasthttp.StatusNoContent)
}

func (srv *Server) handleReport(ctx *fasthttp.RequestCtx) {
	var req phttp.ReportRequest
	err := json.Unmarshal(ctx.PostBody(), &req)
	if err != nil {
		ctx.Error(err.Error(), fasthttp.StatusBadRequest)
		return
	}
	start, stop := req.Period()
	lines := srv.backend.Report(req.Tag, start, stop)
	body, err := json.Marshal(lines)
	Check(err)
	ctx.SetBody(body)
	ctx.SetContentType("application/json")
}

func Serve(args []string) {
	fs := flag.NewFlagSet("server", flag.ExitOnError)
	backend := fs.String("backend", "leveldb", "Backend type (leveldb | influxdb | memdb)")
	cfg := pimco.LoadConfigEx(fs, args...)
	cfg.Server.Backend = *backend
	log.Println(cfg)
	srv := NewServer(cfg)
	// serve metrics
	prom.Setup(cfg.Metrics.EnableHist, cfg.Metrics.EnableSum)
	http.Handle("/metrics", promhttp.Handler())
	go func() {
		log.Fatalln(http.ListenAndServe(cfg.Metrics.Addr, nil))
	}()
	err := fasthttp.ListenAndServe(cfg.Server.Addr, srv.Route)
	// TODO - handle graceful shutdown - drain save channels first
	Check(err)

}
