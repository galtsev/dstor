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
	Serializer string
}

type GenConfig struct {
	Start string
	Step  int //ms
	Count int
	Tags  int // number of tags
}

type Config struct {
	Influx     InfluxConfig
	Kafka      KafkaConfig
	Gen        GenConfig
	OneShot    bool `yaml:"one_shot"`
	HTTPServer struct {
		Addr string
	}
}

func (cfg Config) String() string {
	return fmt.Sprintf("Config<Influx:<URL:%s/%s/%s; BatchSize: %d>; Count: %d>",
		cfg.Influx.URL,
		cfg.Influx.Database,
		cfg.Influx.Measurement,
		cfg.Influx.BatchSize,
		cfg.Gen.Count,
	)
}

func NewConfig() *Config {
	cfg := Config{
		Kafka: KafkaConfig{
			Hosts:      []string{"192.168.0.2:9092"},
			Topic:      "test",
			Partitions: []int32{0, 1, 2, 3},
			BatchSize:  200,
			FlushDelay: 50,
			Serializer: "msgp",
		},
		Influx: InfluxConfig{
			URL:         "http://192.168.0.2:8086",
			Database:    "test",
			Measurement: "ms",
			BatchSize:   1000,
			FlushDelay:  50,
		},
		Gen: GenConfig{
			Start: "2017-04-06 10:00",
			Step:  400,
			Count: 10000,
			Tags:  20,
		},
		OneShot: false,
	}
	cfg.HTTPServer.Addr = "localhost:8787"

	return &cfg
}

func LoadConfig(args ...string) Config {
	return LoadConfigEx(nil, args...)
}

func LoadConfigEx(fs *flag.FlagSet, args ...string) Config {
	cfg := NewConfig()
	data, err := ioutil.ReadFile("pimco.yaml")
	Check(err)
	Check(yaml.Unmarshal(data, cfg))
	if fs == nil {
		fs = flag.NewFlagSet("base", flag.ExitOnError)
	}
	fs.IntVar(&cfg.Influx.BatchSize, "ibs", cfg.Influx.BatchSize, "Influx batch size")
	fs.IntVar(&cfg.Gen.Count, "count", cfg.Gen.Count, "Count")
	fs.Parse(args)
	return *cfg
}
