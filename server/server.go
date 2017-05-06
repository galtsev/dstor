package server

import (
	"dan/pimco"
	. "dan/pimco/base"
	"dan/pimco/conf"
	"dan/pimco/model"
	"dan/pimco/phttp"
	"dan/pimco/prom"
	"dan/pimco/serializer"
	"encoding/json"
	"github.com/valyala/fasthttp"
	"time"
)

type Server struct {
	cfg      conf.ServerConfig
	storage  pimco.Storage
	reporter pimco.Reporter
	json     serializer.Serializer
}

func NewServer(cfg conf.ServerConfig, storage pimco.Storage, reporter pimco.Reporter) *Server {
	server := Server{
		cfg:      cfg,
		json:     serializer.NewSerializer("easyjson"),
		storage:  storage,
		reporter: reporter,
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
	// we either accept writes through http or consume kafka, not both
	if srv.storage == nil {
		ctx.NotFound()
		return
	}
	var samples model.Samples
	err := srv.json.Unmarshal(ctx.PostBody(), &samples)
	if err != nil {
		ctx.Error(err.Error(), fasthttp.StatusBadRequest)
		return
	}
	for _, sample := range samples {
		srv.storage.AddSample(&sample, 0)
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
	lines := srv.reporter.Report(req.Tag, start, stop)
	body, err := json.Marshal(lines)
	Check(err)
	ctx.SetBody(body)
	ctx.SetContentType("application/json")
}
