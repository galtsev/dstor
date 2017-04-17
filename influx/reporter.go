package influx

import (
	"dan/pimco"
	. "dan/pimco/base"
	"encoding/json"
	"fmt"
	"github.com/influxdata/influxdb/client/v2"
	"time"
)

type Reporter struct {
	conn     client.Client
	database string
}

func NewReporter(cfg pimco.InfluxConfig) pimco.Reporter {
	conf := client.HTTPConfig{Addr: cfg.URL}
	conn, err := client.NewHTTPClient(conf)
	Check(err)
	return &Reporter{
		conn:     conn,
		database: cfg.Database,
	}
}

func (r Reporter) Report(tag string, start, stop time.Time) []pimco.ReportLine {
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
	q := client.NewQueryWithParameters(ql, r.database, "ns", params)
	resp, err := r.conn.Query(q)
	Check(err)
	if resp.Error() != nil {
		panic(resp.Error())
	}
	var res []pimco.ReportLine
	for _, row := range resp.Results[0].Series[0].Values {
		ts, err := row[0].(json.Number).Int64()
		Check(err)
		r := pimco.ReportLine{
			TS: ts,
		}
		for i, v := range row[1:] {
			vv, err := v.(json.Number).Float64()
			Check(err)
			r.Values[i] = vv
		}
		res = append(res, r)
	}
	return res
}
