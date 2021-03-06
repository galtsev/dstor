package conf

import (
	"flag"
	"fmt"
	. "github.com/galtsev/dstor/base"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
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
	// register storage node if last consumed offset+DoneOffset>=HighWaterMark for this partition
	DoneOffset int64
}

type ZookeeperConfig struct {
	Servers []string
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
	Host        string
	BatchSize   int `yaml:"batch_size"`
	Concurrency int `yaml:"concurrency`
}

type ServerConfig struct {
	Addr              string
	Storage           string
	Reporter          string
	ConsumePartitions []int32 `yaml:"consume_partitions"`
	AdvertizeHost     string
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
	Batch         BatchConfig
	NumPartitions int `yaml:"num_partitions"`
	Partitions    []int32
}

type MetricsConfig struct {
	Addr       string
	EnableHist bool
	EnableSum  bool
}

type BatchConfig struct {
	BatchSize  int `yaml:"batch_size"`
	FlushDelay int `yaml:"flush_delay"`
}

type Config struct {
	Influx    InfluxConfig
	Kafka     KafkaConfig
	Zookeeper ZookeeperConfig
	Gen       GenConfig
	Server    ServerConfig
	Client    ClientConfig
	Metrics   MetricsConfig
	Leveldb   LeveldbConfig
	OneShot   bool   `yaml:"one_shot"`
	FilePath  string `yaml:"file_path"`
}

func (cfg Config) String() string {
	return fmt.Sprintf("Config<Gen.Backend: %s; Gen.Count: %d; FilePath:%s>",
		cfg.Gen.Backend,
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
			DoneOffset:    100,
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
			Addr:          "0.0.0.0:8787",
			Storage:       "leveldb",
			Reporter:      "leveldb",
			AdvertizeHost: "localhost:8787",
		},
		Metrics: MetricsConfig{
			Addr:       ":8789",
			EnableHist: true,
			EnableSum:  false,
		},
		Client: ClientConfig{
			Host:        "localhost:8787",
			BatchSize:   10,
			Concurrency: 20,
		},
		Leveldb: LeveldbConfig{
			Path: "/mnt/leveldb",
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

func Load(cfg *Config) {
	var fileName string = os.Getenv("DSTOR_CONFIG")
	if fileName == "" {
		for _, fn := range []string{"dstor.yaml", "/etc/dstor.yaml"} {
			if _, err := os.Stat(fn); !os.IsNotExist(err) {
				log.Printf("found %s", fn)
				fileName = fn
				break
			}
		}
	}
	if fileName == "" {
		log.Fatalf("Config file not found")
	}
	data, err := ioutil.ReadFile(fileName)
	Check(err)
	Check(yaml.Unmarshal(data, cfg))
	zk_hosts := os.Getenv("DSTOR_ZK_HOST")
	if zk_hosts != "" {
		cfg.Zookeeper.Servers = strings.Split(zk_hosts, ",")
	}
	kafka_hosts := os.Getenv("DSTOR_KAFKA_HOST")
	if kafka_hosts != "" {
		cfg.Kafka.Hosts = strings.Split(kafka_hosts, ",")
	}
	advertizeHost := os.Getenv("DSTOR_ADVERTIZE_HOST")
	if advertizeHost != "" {
		cfg.Server.AdvertizeHost = advertizeHost
	}
	numPartitions := os.Getenv("DSTOR_NUM_PARTITIONS")
	if numPartitions != "" {
		np, err := strconv.Atoi(numPartitions)
		Check(err)
		cfg.Kafka.NumPartitions = np
		cfg.Leveldb.NumPartitions = np
	}

}

func Parse(cfg *Config, args []string, fs *flag.FlagSet, with ...string) {
	var genDuration time.Duration
	var genStart string
	var withGen bool
	for _, ff := range with {
		switch ff {
		case "gen":
			fs.IntVar(&cfg.Gen.Count, "gen.count", cfg.Gen.Count, "Num samples")
			fs.StringVar(&genStart, "gen.start", "", "Generation period start date YYYY-mm-dd HH:MM")
			fs.DurationVar(&genDuration, "gen.duration", time.Duration(24)*time.Hour, "Generation period duration <int>{h|m|s}")
			fs.IntVar(&cfg.Gen.Tags, "gen.tags", cfg.Gen.Tags, "Number of tags to generate")
			fs.StringVar(&cfg.Gen.Backend, "backend", cfg.Gen.Backend, "backend")
			withGen = true
		}

	}
	fs.Parse(args)
	if withGen {
		if genStart != "" {
			dt, err := time.Parse(DATE_FORMAT, genStart)
			Check(err)
			cfg.Gen.Start = genStart
			cfg.Gen.End = dt.Add(genDuration).Format(DATE_FORMAT)
		}
	}
}
