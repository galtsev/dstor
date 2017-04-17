package command

import (
	"dan/pimco"
	. "dan/pimco/base"
	"dan/pimco/influx"
	"flag"
	"fmt"
	"time"
)

func QueryFlux(args []string) {
	fs := flag.NewFlagSet("query", flag.ExitOnError)
	startStr := fs.String("start", "", "Start of reporting period, YYYY-MM-DD HH:MM")
	endStr := fs.String("end", "", "End of reporting period, YYYY-MM-DD HH:MM")
	tag := fs.String("tag", "", "tag to report")
	cfg := pimco.LoadConfigEx(fs, args...)
	fmt.Println(cfg)
	start, err := time.Parse(date_format, *startStr)
	Check(err)
	end, err := time.Parse(date_format, *endStr)
	Check(err)
	fmt.Printf("Report for period from %v to %v\n", start, end)
	reporter := influx.NewReporter(cfg.Influx)
	for _, line := range reporter.Report(*tag, start, end) {
		fmt.Println(time.Unix(0, line.TS).UTC(), line.Values)
	}

}
