package pimco

import (
	. "dan/pimco/base"
	"flag"
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"time"
)

type InfluxConfig struct {
	URL         string
	Database    string
	Measurement string
	BatchSize   int `yaml:"batch_size"`
	FlushDelay  int `yaml:"flush_delay"` //milliseconds
}

type KafkaConfig struct {
	Hosts         []string
	Topic         string
	NumPartitions int     `yaml:"num_partitions"` // total number of partitions in this topic
	Partitions    []int32 // partitions to consume
	BatchSize     int     `yaml:"batch_size"`
	FlushDelay    int     `yaml:"flush_delay"` //milliseconds
	Serializer    string
}

type GenConfig struct {
	Mode  string
	Start string
	End   string
	Count int
	Tags  int // number of tags
}

func (cfg GenConfig) Period() (start, end time.Time) {
	ts, err := time.Parse(DATE_FORMAT, cfg.Start)
	Check(err)
	endTS, err := time.Parse(DATE_FORMAT, cfg.End)
	Check(err)
	return ts, endTS
}

type ClientConfig struct {
	Host      string
	BatchSize int `yaml:"batch_size"`
}

type ReceptorServerConfig struct {
	Addr string
}

type ReportingServerConfig struct {
	Addr string
}

type LeveldbOptions struct {
	WriteBufferMb                 int
	CompactionTableSizeMb         int
	CompactionTotalSizeMb         int
	CompactionTotalSizeMultiplier float64
	WriteL0SlowdownTrigger        int
	WriteL0PauseTrigger           int
	CompactionL0Trigger           int
}

type LeveldbConfig struct {
	Path       string
	BatchSize  int
	FlushDelay int
	Opts       LeveldbOptions
}

type MetricsConfig struct {
	Addr       string
	EnableHist bool
	EnableSum  bool
}

type Config struct {
	Influx          InfluxConfig
	Kafka           KafkaConfig
	Gen             GenConfig
	ReportingServer ReportingServerConfig
	ReceptorServer  ReceptorServerConfig
	Client          ClientConfig
	Metrics         MetricsConfig
	Leveldb         LeveldbConfig
	OneShot         bool `yaml:"one_shot"`
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
			Hosts:         []string{"192.168.0.2:9092"},
			Topic:         "test",
			NumPartitions: 4,
			Partitions:    []int32{0},
			BatchSize:     200,
			FlushDelay:    50,
			Serializer:    "msgp",
		},
		Influx: InfluxConfig{
			URL:         "http://192.168.0.2:8086",
			Database:    "test",
			Measurement: "ms",
			BatchSize:   1000,
			FlushDelay:  50,
		},
		Gen: GenConfig{
			Start: "2017-04-06 00:00",
			End:   "2017-04-07 00:00",
			Count: 10000,
			Tags:  20,
			Mode:  "random",
		},
		ReceptorServer: ReceptorServerConfig{
			Addr: "localhost:8787",
		},
		ReportingServer: ReportingServerConfig{
			Addr: "localhost:8788",
		},
		Metrics: MetricsConfig{
			Addr:       ":8789",
			EnableHist: true,
			EnableSum:  false,
		},
		Client: ClientConfig{
			Host:      "localhost:8787",
			BatchSize: 10,
		},
		Leveldb: LeveldbConfig{
			Path:       "/home/dan/data/leveldb",
			BatchSize:  1000,
			FlushDelay: 50,
			Opts: LeveldbOptions{
				WriteBufferMb:                 64,
				CompactionTableSizeMb:         4,
				CompactionTotalSizeMb:         40,
				CompactionTotalSizeMultiplier: 4,
				CompactionL0Trigger:           2,
				WriteL0SlowdownTrigger:        4,
				WriteL0PauseTrigger:           12,
			},
		},
		OneShot: false,
	}

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
