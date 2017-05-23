package command

import (
	"flag"
	"fmt"
	"github.com/galtsev/dstor/api"
	"github.com/galtsev/dstor/conf"
	"os"
	"time"
)

func expectedValue(ts time.Time) float64 {
	return float64(ts.UnixNano()) / 1000000000
}

func CheckReport(args []string) {
	cfg := *conf.NewConfig()
	conf.Load(&cfg)
	fs := flag.NewFlagSet("client", flag.ExitOnError)
	fs.StringVar(&cfg.Client.Host, "host", "localhost:8787", "server host:port")
	fs.Parse(args)
	client := api.NewClient(cfg.Client)
	start, end := cfg.Gen.Period()
	tag := "tag1"
	res := client.Report(tag, start, end)
	duration := end.Sub(start)
	reportStep := duration / time.Duration(100)
	for i, line := range res {
		expectedTS := start.Add(reportStep * time.Duration(i))
		vmin := expectedValue(expectedTS)
		vmax := expectedValue(expectedTS.Add(reportStep))
		if line.TS != expectedTS.UnixNano() {
			fmt.Printf("Wrong timestamp. i:%d; expected: %v; actual: %v\n", i, expectedTS.UnixNano(), line.TS)
			os.Exit(1)
		}
		actual := line.Values[0]
		if actual < vmin || actual > vmax {
			fmt.Printf("Wrong value. i:%d; expected range: %v-%v; actual: %v\n", i, vmin, vmax, actual)
			os.Exit(1)
		}
	}
	fmt.Println("ok")
}
