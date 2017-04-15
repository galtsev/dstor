package temp

import (
	"dan/pimco"
	. "dan/pimco/base"
	"fmt"
	"github.com/influxdata/influxdb/client/v2"
	"time"
)

func Run1(args []string) {
	cfg := pimco.LoadConfig(args...)
	fmt.Println(cfg)
	fieldSet := []struct {
		key   string
		scale float64
	}{
		{"v1", 1},
		{"v2", 2},
		{"v3", 3},
		{"v4", 4},
		{"v5", 5},
		{"v6", 6},
		{"v7", 7},
		{"v8", 8},
		{"v9", 9},
		{"v10", 10},
	}
	conn, err := client.NewHTTPClient(client.HTTPConfig{Addr: cfg.Influx.URL})
	Check(err)
	defer conn.Close()
	dt, err := time.Parse(date_format, start)
	Check(err)
	bpConfig := client.BatchPointsConfig{Database: cfg.Influx.Database}
	tags := map[string]string{}
	for i := 0; i < cfg.Count/cfg.Influx.BatchSize; i++ {
		batch, err := client.NewBatchPoints(bpConfig)
		Check(err)
		for j := 0; j < cfg.Influx.BatchSize; j++ {
			v := float64(dt.UnixNano())
			fields := make(map[string]interface{})
			for _, f := range fieldSet {
				fields[f.key] = v * f.scale
			}

			p, err := client.NewPoint(cfg.Influx.Measurement, tags, fields, dt)
			Check(err)
			batch.AddPoint(p)
			dt = dt.Add(step * time.Millisecond)
		}
		conn.Write(batch)
	}
}
