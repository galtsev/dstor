package command

import (
	"dan/pimco"
	. "dan/pimco/base"
	"dan/pimco/ldb"
	"dan/pimco/model"
	"dan/pimco/phttp"
	"dan/pimco/serializer"
	"encoding/json"
	"github.com/valyala/fasthttp"
	"log"
	"time"
)

type StandaloneLeveldbServer struct {
	channels    []*pimco.BatchWriter
	reporters   []pimco.Reporter
	json        serializer.Serializer
	partitioner func(string) int32
}

func NewStandaloneLeveldbServer(cfg pimco.Config) *StandaloneLeveldbServer {
	server := StandaloneLeveldbServer{
		json:        serializer.NewSerializer("easyjson"),
		partitioner: pimco.MakePartitioner(cfg.Kafka.NumPartitions),
	}
	for p := 0; p < cfg.Kafka.NumPartitions; p++ {
		db := ldb.Open(cfg.Leveldb, int32(p))
		server.reporters = append(server.reporters, db)
		writer := pimco.NewWriter(db, cfg.Leveldb.BatchSize, time.Duration(cfg.Leveldb.FlushDelay)*time.Millisecond)
		server.channels = append(server.channels, writer)
	}
	return &server
}

func (srv *StandaloneLeveldbServer) Route(ctx *fasthttp.RequestCtx) {
	switch string(ctx.Path()) {
	case "/write":
		srv.handleWrite(ctx)
	case "/report":
		srv.handleReport(ctx)
	default:
		ctx.NotFound()
	}
}

func (srv *StandaloneLeveldbServer) handleWrite(ctx *fasthttp.RequestCtx) {
	var samples model.Samples
	err := srv.json.Unmarshal(ctx.PostBody(), &samples)
	if err != nil {
		ctx.Error(err.Error(), fasthttp.StatusBadRequest)
		return
	}
	for _, sample := range samples {
		p := srv.partitioner(sample.Tag)
		srv.channels[p].Write(&sample)
	}
	ctx.SetStatusCode(fasthttp.StatusNoContent)
}

func (srv *StandaloneLeveldbServer) handleReport(ctx *fasthttp.RequestCtx) {
	var req phttp.ReportRequest
	err := json.Unmarshal(ctx.PostBody(), &req)
	if err != nil {
		ctx.Error(err.Error(), fasthttp.StatusBadRequest)
		return
	}
	start, stop := req.Period()
	db := srv.reporters[srv.partitioner(req.Tag)]
	lines := db.Report(req.Tag, start, stop)
	body, err := json.Marshal(lines)
	Check(err)
	ctx.SetBody(body)
	ctx.SetContentType("application/json")
}

func LeveldbStandaloneServer(args []string) {
	cfg := pimco.LoadConfig(args...)
	log.Println(cfg)
	srv := NewStandaloneLeveldbServer(cfg)
	err := fasthttp.ListenAndServe(cfg.ReceptorServer.Addr, srv.Route)
	// TODO - handle graceful shutdown - drain save channels first
	Check(err)

}
