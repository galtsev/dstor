package server

import (
	"encoding/json"
	"github.com/galtsev/dstor"
	. "github.com/galtsev/dstor/base"
	"github.com/galtsev/dstor/conf"
	"github.com/galtsev/dstor/model"
	"github.com/galtsev/dstor/phttp"
	"github.com/galtsev/dstor/prom"
	"github.com/galtsev/dstor/serializer"
	"github.com/valyala/fasthttp"
	"log"
	"strconv"
	"time"
)

type Server struct {
	cfg      conf.ServerConfig
	storage  dstor.Storage
	reporter dstor.Reporter
	json     serializer.Serializer
}

func NewServer(cfg conf.ServerConfig, storage dstor.Storage, reporter dstor.Reporter) *Server {
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
	case "/save":
		srv.handleWrite(ctx)
		path = "write"
	case "/batch":
		srv.handleBatch(ctx)
	case "/api":
		srv.handleReport(ctx)
		path = "report"
	case "/ping":
		srv.handlePing(ctx)
	default:
		ctx.NotFound()
	}
	if path != "" {
		prom.RequestTime(path, time.Now().Sub(start))
	}
}

func (srv *Server) handlePing(ctx *fasthttp.RequestCtx) {
	ctx.SetBody([]byte("pong"))
}

func (srv *Server) handleBatch(ctx *fasthttp.RequestCtx) {
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

func (srv *Server) handleWrite(ctx *fasthttp.RequestCtx) {
	// we either accept writes through http or consume kafka, not both
	if srv.storage == nil {
		ctx.NotFound()
		return
	}
	var sample model.Sample
	err := srv.json.Unmarshal(ctx.PostBody(), &sample)
	if err != nil {
		ctx.Error(err.Error(), fasthttp.StatusBadRequest)
		return
	}
	srv.storage.AddSample(&sample, 0)
	ctx.SetStatusCode(fasthttp.StatusNoContent)
}

func toInt64(data []byte) (v int64, err error) {
	v, err = strconv.ParseInt(string(data), 10, 64)
	return v, err
}

func (srv *Server) handleReport(ctx *fasthttp.RequestCtx) {
	args := ctx.QueryArgs()
	log.Println(args)
	if !args.Has("start") {
		ctx.Error("start parameter missing", fasthttp.StatusBadRequest)
		return
	}
	if !args.Has("end") {
		ctx.Error("end parameter missing", fasthttp.StatusBadRequest)
		return
	}
	if !args.Has("tag") {
		ctx.Error("tag parameter missing", fasthttp.StatusBadRequest)
		return
	}
	start, err := toInt64(args.Peek("start"))
	if err != nil {
		ctx.Error("bad value for start, expected int", fasthttp.StatusBadRequest)
		return
	}
	end, err := toInt64(args.Peek("end"))
	if err != nil {
		ctx.Error("bad value for end, expected int", fasthttp.StatusBadRequest)
		return
	}
	tag := string(args.Peek("tag"))
	resp := phttp.ReportResponse{
		Tag:   tag,
		Start: start,
		End:   end,
	}
	startTime := time.Unix(0, start)
	endTime := time.Unix(0, end)
	resp.Samples = srv.reporter.Report(tag, startTime, endTime)
	body, err := json.Marshal(resp)
	Check(err)
	ctx.SetBody(body)
	ctx.SetContentType("application/json")
}
