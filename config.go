package pimco

import (
	. "dan/pimco/base"
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

type KafkaConfig struct {
	Hosts      []string
	Topic      string
	Partitions []int32
	BatchSize  int `yaml:"batch_size"`
	FlushDelay int `yaml:"flush_delay"` //milliseconds
}

type Config struct {
	Influx  InfluxConfig
	Kafka   KafkaConfig
	Count   int
	Tags    int
	OneShot bool `yaml:"one_shot"`
	Gen     struct {
		Start string
		Step  int
	}
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
	return LoadConfigEx(nil, args...)
}

func LoadConfigEx(fs *flag.FlagSet, args ...string) Config {
	var cfg Config
	data, err := ioutil.ReadFile("pimco.yaml")
	Check(err)
	Check(yaml.Unmarshal(data, &cfg))
	if fs == nil {
		fs = flag.NewFlagSet("base", flag.ExitOnError)
	}
	fs.IntVar(&cfg.Influx.BatchSize, "ibs", cfg.Influx.BatchSize, "Influx batch size")
	fs.IntVar(&cfg.Count, "count", cfg.Count, "Count")
	fs.Parse(args)
	return cfg
}
