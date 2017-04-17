package command

import (
	"dan/pimco"
	"dan/pimco/influx"
	"fmt"
	"time"
)

func Run2(args []string) {
	cfg := pimco.LoadConfig(args...)
	fmt.Println(cfg)
	fluxWriter := influx.NewWriter(cfg.Influx)
	w := pimco.NewWriter(fluxWriter, cfg.Influx.BatchSize, time.Duration(cfg.Influx.FlushDelay)*time.Millisecond)
	gen := pimco.NewGenerator(cfg.Gen)

	for gen.Next() {
		w.Write(gen.Sample())
	}
	w.Close()

}
