package server

import (
	"bytes"
	"encoding/json"
	"github.com/galtsev/dstor"
	. "github.com/galtsev/dstor/base"
	"github.com/galtsev/dstor/conf"
	"github.com/galtsev/dstor/model"
	"github.com/galtsev/dstor/phttp"
	"github.com/galtsev/dstor/prom"
	"github.com/galtsev/dstor/serializer"
	"github.com/valyala/fasthttp"
	"html/template"
	"log"
	"strconv"
	"time"
)

var tplDemo = `<html>
<style>
table {
    border-collapse: collapse;
}

table, th, td {
    border: 1px solid gray;
    padding: 2px;
}
</style>
<body>
<table>
<th>
	<td>1</td>
	<td>2</td>
</th>
{{range .Samples }}
	<tr>
		<td>{{ .TS }}</td>
		{{range .Values }}<td>{{printf "%.1f" .}}</td>{{end}}
	</tr>
{{end}}
</table>
</body>
</html>
`

type ErrBadRequest string

func (e ErrBadRequest) Error() string {
	return string(e)
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

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
	defer func() {
		if err := recover(); err != nil {
			switch e := err.(type) {
			case ErrBadRequest:
				ctx.Error(e.Error(), fasthttp.StatusBadRequest)
			case error:
				ctx.Error(e.Error(), fasthttp.StatusInternalServerError)
			default:
				panic(err)
			}

		}
	}()
	start := time.Now()
	var path string
	var logged bool = false
	switch string(ctx.Path()) {
	case "/save":
		srv.handleWrite(ctx)
		path = "write"
	case "/batch":
		srv.handleBatch(ctx)
	case "/api":
		srv.handleReport(ctx)
		path = "report"
		logged = true
	case "/demo":
		srv.handleDemo(ctx)
		logged = true
	case "/ping":
		srv.handlePing(ctx)
	default:
		ctx.NotFound()
	}
	if logged {
		log.Print(ctx.URI().String())
	}
	if path != "" {
		prom.RequestTime(path, time.Now().Sub(start))
	}
}

func mustGetTimeArg(args *fasthttp.Args, name string) time.Time {
	if !args.Has(name) {
		panic(ErrBadRequest("Missing required argument: " + name))
	}
	v, err := strconv.ParseInt(string(args.Peek(name)), 10, 64)
	check(err)
	return time.Unix(0, v)
}

func mustGetStringArg(args *fasthttp.Args, name string) string {
	if !args.Has(name) {
		panic(ErrBadRequest("Missing required argument: " + name))
	}
	return string(args.Peek(name))
}

func (srv *Server) handleDemo(ctx *fasthttp.RequestCtx) {
	args := ctx.QueryArgs()
	start := mustGetTimeArg(args, "start")
	end := mustGetTimeArg(args, "end")
	tag := mustGetStringArg(args, "tag")
	resp := phttp.ReportResponse{
		Tag:   tag,
		Start: start.UnixNano(),
		End:   end.UnixNano(),
	}
	samples := srv.reporter.Report(tag, start, end)

	// put 0 to lines with same Values as in previous line
	// to visually emphasize not yet generated period
	var nsamples []dstor.ReportLine
	for i, line := range samples {
		nline := line
		if i > 0 && line.Values[0] == samples[i-1].Values[0] {
			for j := range line.Values {
				nline.Values[j] = 0
			}
		}
		nsamples = append(nsamples, nline)
	}
	resp.Samples = nsamples
	ctx.SetContentType("text/html")
	tpl, err := template.New("report").Parse(tplDemo)
	Check(err)
	var buf bytes.Buffer
	tpl.Execute(&buf, resp)
	ctx.SetBody(buf.Bytes())
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

func (srv *Server) handleReport(ctx *fasthttp.RequestCtx) {
	args := ctx.QueryArgs()
	start := mustGetTimeArg(args, "start")
	end := mustGetTimeArg(args, "end")
	tag := mustGetStringArg(args, "tag")
	resp := phttp.ReportResponse{
		Tag:   tag,
		Start: start.UnixNano(),
		End:   end.UnixNano(),
	}
	resp.Samples = srv.reporter.Report(tag, start, end)
	body, err := json.Marshal(resp)
	check(err)
	ctx.SetBody(body)
	ctx.SetContentType("application/json")
}
