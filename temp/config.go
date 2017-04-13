package temp

import (
	. "dan/pimco/util"
	"flag"
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

type InfluxConfig struct {
	URL         string
	Database    string
	Measurement string
	BatchSize   int `yaml:"batch_size"`
	FlushDelay  int `yaml:"flush_delay"` //milliseconds
}

type Config struct {
	Influx InfluxConfig
	Count  int
	Tags   int
}

func (cfg Config) String() string {
	return fmt.Sprintf("Config<Influx:<URL:%s/%s/%s; BatchSize: %d>; Count: %d>",
		cfg.Influx.URL,
		cfg.Influx.Database,
		cfg.Influx.Measurement,
		cfg.Influx.BatchSize,
		cfg.Count,
	)
}

func LoadConfig(args ...string) Config {
	var cfg Config
	data, err := ioutil.ReadFile("pimco.yaml")
	Check(err)
	Check(yaml.Unmarshal(data, &cfg))
	if len(args) > 0 {
		fs := flag.NewFlagSet("gen", flag.ExitOnError)
		fs.IntVar(&cfg.Influx.BatchSize, "ibs", cfg.Influx.BatchSize, "Influx batch size")
		fs.IntVar(&cfg.Count, "count", cfg.Count, "Count")
		fs.Parse(args)
	}
	return cfg
}
