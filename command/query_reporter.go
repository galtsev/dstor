package command

import (
	"flag"
	"fmt"
	"github.com/galtsev/dstor/api"
	. "github.com/galtsev/dstor/base"
	"github.com/galtsev/dstor/conf"
	"math"
	"math/rand"
	"time"
)

func QueryReporter(args []string) {
	cfg := conf.NewConfig()
	conf.Load(cfg)
	fs := flag.NewFlagSet("query", flag.ExitOnError)
	startStr := fs.String("start", cfg.Gen.Start, "Start of reporting period, YYYY-MM-DD HH:MM")
	endStr := fs.String("end", cfg.Gen.End, "End of reporting period, YYYY-MM-DD HH:MM")
	bench := fs.Int("bench", 0, "Run N times, report accumulated time")
	tag := fs.String("tag", "tag1", "tag to report")
	fs.Parse(args)
	fmt.Println(cfg)
	fmt.Printf("Report for period from %s to %s\n", *startStr, *endStr)
	start, err := time.Parse(DATE_FORMAT, *startStr)
	Check(err)
	stop, err := time.Parse(DATE_FORMAT, *endStr)
	Check(err)
	client := api.NewClient(cfg.Client)
	pp := 1 / math.Log(1-0.01)
	nTags := cfg.Gen.Tags
	tags := make([]string, nTags)
	for i := range tags {
		tags[i] = fmt.Sprintf("tag%d", i)
	}
	randn := func() int {
		var v float64
		for v == 0 {
			v = rand.Float64()
			index := int(pp * math.Log(v))
			if index < nTags {
				return index
			}
		}
		return 0
	}
	if *bench == 0 {
		lines := client.Report(*tag, start, stop)
		for _, line := range lines {
			fmt.Println(time.Unix(0, line.TS).UTC(), line.Values)
		}
	} else {
		for i := 0; i < *bench; i++ {
			atag := tags[randn()]
			_ = client.Report(atag, start, stop)
		}
	}
}
