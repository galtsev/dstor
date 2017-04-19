package command

import (
	"dan/pimco"
	"dan/pimco/api"
	. "dan/pimco/base"
	"flag"
	"fmt"
	"time"
)

func QueryReporter(args []string) {
	fs := flag.NewFlagSet("query", flag.ExitOnError)
	startStr := fs.String("start", "", "Start of reporting period, YYYY-MM-DD HH:MM")
	endStr := fs.String("end", "", "End of reporting period, YYYY-MM-DD HH:MM")
	bench := fs.Int("bench", 0, "Run N times, report accumulated time")
	tag := fs.String("tag", "", "tag to report")
	cfg := pimco.LoadConfigEx(fs, args...)
	fmt.Println(cfg)
	fmt.Printf("Report for period from %s to %s\n", *startStr, *endStr)
	start, err := time.Parse(DATE_FORMAT, *startStr)
	Check(err)
	stop, err := time.Parse(DATE_FORMAT, *endStr)
	Check(err)
	client := api.NewClient(cfg.Client)
	if *bench == 0 {
		lines := client.Report(*tag, start, stop)
		for _, line := range lines {
			fmt.Println(time.Unix(0, line.TS).UTC(), line.Values)
		}
	} else {
		for i := 0; i < *bench; i++ {
			_ = client.Report(*tag, start, stop)
		}
	}
}
