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
	dt, err := time.Parse(date_format, cfg.Gen.Start)
	Check(err)
	gen := pimco.NewGenerator(cfg.Gen.Tags, dt.UnixNano(), cfg.Gen.Step)
	for i := 0; i < cfg.Gen.Count; i++ {
		w.Write(gen.Next())
	}
	w.Close()

}
