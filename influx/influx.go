package influx

import (
	"dan/pimco"
	. "dan/pimco/base"
	"dan/pimco/conf"
	"dan/pimco/model"
	"encoding/json"
	"fmt"
	"github.com/influxdata/influxdb/client/v2"
	"time"
)

type Influx struct {
	conn     client.Client
	database string
	writer   *pimco.BatchWriter
	bpConfig client.BatchPointsConfig
	batch    client.BatchPoints
}

func New(cfg conf.InfluxConfig, batchConfig conf.BatchConfig) *Influx {
	w := Influx{
		bpConfig: client.BatchPointsConfig{Database: cfg.Database},
		database: cfg.Database,
	}
	conn, err := client.NewHTTPClient(client.HTTPConfig{Addr: cfg.URL})
	Check(err)
	w.conn = conn
	w.writer = pimco.NewWriter(&w, batchConfig)
	return &w
}

func (w *Influx) Add(sample *model.Sample) {
	if w.batch == nil {
		batch, err := client.NewBatchPoints(w.bpConfig)
		Check(err)
		w.batch = batch
	}
	AddSample(sample, w.batch)
}

func (w *Influx) Flush() {
	if w.batch != nil {
		Check(w.conn.Write(w.batch))
		w.batch = nil
	}
}

func (w *Influx) Close() {
	w.Flush()
	w.conn.Close()
}

func (w *Influx) AddSample(sample *model.Sample) {
	w.writer.Write(sample)
}

func (w *Influx) Report(tag string, start, stop time.Time) []pimco.ReportLine {
	stime, etime := start.UnixNano(), stop.UnixNano()
	step := (etime - stime) / (10 * 1000 * 1000 * 1000)
	ql := fmt.Sprintf(`select 
        last(V1), last(V2), last(V3), last(V4), last(V5),
        last(V6), last(V7), last(V8), last(V9), last(V10)
        from ms
        where time>=$start_time and time<=$end_time
            and "tag"=$tag
        group by time(%ds) fill(previous)
    `, step)
	params := map[string]interface{}{
		"start_time": stime,
		"end_time":   etime,
		"tag":        tag,
	}
	q := client.NewQueryWithParameters(ql, w.database, "ns", params)
	resp, err := w.conn.Query(q)
	Check(err)
	if resp.Error() != nil {
		panic(resp.Error())
	}
	var res []pimco.ReportLine
	for _, row := range resp.Results[0].Series[0].Values {
		ts, err := row[0].(json.Number).Int64()
		Check(err)
		line := pimco.ReportLine{
			TS: ts,
		}
		for i, v := range row[1:] {
			vv, err := v.(json.Number).Float64()
			Check(err)
			line.Values[i] = vv
		}
		res = append(res, line)
	}
	return res
}

func AddSample(sample *model.Sample, batch client.BatchPoints) {
	fields := make(map[string]interface{})
	for idx, fn := range FIELD_NAMES {
		fields[fn] = sample.Values[idx]
	}
	point, _ := client.NewPoint("ms", map[string]string{"tag": sample.Tag}, fields, time.Unix(0, sample.TS))
	batch.AddPoint(point)
}

func init() {
	pimco.RegisterBackend("influxdb", func(cfg conf.Config) pimco.Backend {
		return New(cfg.Influx, cfg.Batch)
	})
}
