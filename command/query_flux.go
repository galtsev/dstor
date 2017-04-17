package command

import (
	"dan/pimco"
	. "dan/pimco/base"
	"encoding/json"
	"fmt"
	"github.com/influxdata/influxdb/client/v2"
)

func printResp(resp *client.Response) {
	if resp.Err != "" {
		fmt.Println("Response err:", resp.Err)
		return
	}
	for _, result := range resp.Results {
		fmt.Println("Result-----------------------------------")
		if result.Err != "" {
			fmt.Println("Result err:", result.Err)
		}
		for _, msg := range result.Messages {
			fmt.Println("Msg: ", msg)
		}
		fmt.Println("Rows+++++")
		for _, row := range result.Series {
			fmt.Println("Row---------------------")
			fmt.Printf("Name: %s; Tags: %v; columns: %v; partial: %t\n", row.Name, row.Tags, row.Columns, row.Partial)
			for _, vlist := range row.Values {
				fmt.Println(vlist)
			}
		}
	}
}

type ReportLine struct {
	TS     int64
	Values [10]float64
}

type Reporter struct {
	conn     client.Client
	database string
}

func NewReporter(cfg pimco.InfluxConfig) *Reporter {
	conf := client.HTTPConfig{Addr: cfg.URL}
	conn, err := client.NewHTTPClient(conf)
	Check(err)
	return &Reporter{
		conn:     conn,
		database: cfg.Database,
	}
}

func (r Reporter) Report(tag string, start, stop int64) []ReportLine {
	ql := `select
        last(V1),
        last(V2),
        last(V3),
        last(V4),
        last(V5),
        last(V6),
        last(V7),
        last(V8),
        last(V9),
        last(V10)
        from ms
        where time>=1491472800000000000 and time<=1491872800000000000
            and "tag"='tag1'
        group by time(40000s)
    `
	q := client.NewQuery(ql, r.database, "ns")
	resp, err := r.conn.Query(q)
	Check(err)
	var res []ReportLine
	for _, row := range resp.Results[0].Series[0].Values {
		ts, err := row[0].(json.Number).Int64()
		Check(err)
		r := ReportLine{
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

func QueryFlux(args []string) {
	cfg := pimco.LoadConfig(args...)
	fmt.Println(cfg)
	reporter := NewReporter(cfg.Influx)
	for i := 0; i < 1000; i++ {
		_ = reporter.Report("tag1", int64(1491472800000000000), int64(1491872800000000000))
	}
	// for _, row := range reporter.Report("tag1", int64(1491472800000000000), int64(1491872800000000000)) {
	// 	fmt.Println(row)
	// }
}
