package temp

import (
	"dan/pimco"
	. "dan/pimco/base"
	"dan/pimco/influx"
	"fmt"
	"time"
)

func Run2(args []string) {
	cfg := pimco.LoadConfig(args...)
	fmt.Println(cfg)
	fluxWriter := influx.NewWriter(cfg.Influx)
	w := pimco.NewWriter(fluxWriter, cfg.Influx.BatchSize, time.Duration(cfg.Influx.FlushDelay)*time.Millisecond)
	dt, err := time.Parse(date_format, start)
	Check(err)
	gen := pimco.NewGenerator(cfg.Tags, dt.UnixNano(), step*1000000)
	for i := 0; i < cfg.Count; i++ {
		w.Write(gen.Next())
	}
	w.Close()

}
