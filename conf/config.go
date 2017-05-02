package conf

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
	Batch       BatchConfig
}

type KafkaConfig struct {
	Hosts         []string
	Topic         string
	NumPartitions int     `yaml:"num_partitions"` // total number of partitions in this topic
	Partitions    []int32 // partitions to consume
	Serializer    string
	Batch         BatchConfig
}

type GenConfig struct {
	Mode    string
	Start   string
	End     string
	Count   int
	Tags    int // number of tags
	Backend string
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

type ServerConfig struct {
	Addr       string
	Storage    string
	Reporter   string
	Partitions []int32
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
	Path          string
	Opts          LeveldbOptions
	TTL           string // sample would be removed on compaction after this period. format as time.Duration.Parse, eg. 600s
	CompactBefore string `yaml:"compact_before"`
	Batch         BatchConfig
}

type MetricsConfig struct {
	Addr       string
	EnableHist bool
	EnableSum  bool
}

type BatchConfig struct {
	BatchSize  int         `yaml:"batch_size"`
	FlushDelay int         `yaml:"flush_delay"`
	OnFlush    func(int64) `yaml:""`
}

type Config struct {
	Influx   InfluxConfig
	Kafka    KafkaConfig
	Gen      GenConfig
	Server   ServerConfig
	Client   ClientConfig
	Metrics  MetricsConfig
	Leveldb  LeveldbConfig
	OneShot  bool   `yaml:"one_shot"`
	FilePath string `yaml:"file_path"`
}

func (cfg Config) String() string {
	return fmt.Sprintf("Config<Influx.URL:%s/%s/%s; BatchSize: %d; Gen.Count: %d; FilePath:%s",
		cfg.Influx.URL,
		cfg.Influx.Database,
		cfg.Influx.Measurement,
		cfg.Gen.Count,
		cfg.FilePath,
	)
}

func NewConfig() *Config {
	batch := BatchConfig{
		BatchSize:  1000,
		FlushDelay: 50,
	}
	cfg := Config{
		Kafka: KafkaConfig{
			Hosts:         []string{"192.168.0.2:9092"},
			Topic:         "test",
			NumPartitions: 4,
			Partitions:    []int32{0},
			Serializer:    "msgp",
			Batch:         batch,
		},
		Influx: InfluxConfig{
			URL:         "http://192.168.0.2:8086",
			Database:    "test",
			Measurement: "ms",
			Batch:       batch,
		},
		Gen: GenConfig{
			Start:   "2017-04-06 00:00",
			End:     "2017-04-07 00:00",
			Count:   10000,
			Tags:    20,
			Mode:    "random",
			Backend: "leveldb",
		},
		Server: ServerConfig{
			Addr:     "localhost:8787",
			Storage:  "leveldb",
			Reporter: "leveldb",
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
			Path: "/home/dan/data/leveldb",
			Opts: LeveldbOptions{
				WriteBufferMb:                 64,
				CompactionTableSizeMb:         4,
				CompactionTotalSizeMb:         40,
				CompactionTotalSizeMultiplier: 4,
				CompactionL0Trigger:           2,
				WriteL0SlowdownTrigger:        4,
				WriteL0PauseTrigger:           12,
			},
			Batch: batch,
		},
		OneShot: false,
	}

	return &cfg
}

func LoadConfig(args ...string) Config {
	fs := flag.NewFlagSet("base", flag.ExitOnError)
	return LoadConfigEx(fs, args...)
}

func LoadConfigEx(fs *flag.FlagSet, args ...string) Config {
	cfg := NewConfig()
	data, err := ioutil.ReadFile("pimco.yaml")
	Check(err)
	Check(yaml.Unmarshal(data, cfg))
	fs.Parse(args)
	return *cfg
}
