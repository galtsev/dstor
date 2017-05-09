package command

import (
	"dan/pimco/conf"
	"dan/pimco/kafka"
	"dan/pimco/util"
	"fmt"
	"github.com/Shopify/sarama"
)

func TopicStats(args []string) {
	cfg := conf.NewConfig()
	conf.Load(cfg)
	fmt.Println(cfg)
	cnt := 0
	tags := make(map[string]int)
	progress := util.NewProgress(100000)
	for ksample := range kafka.ConsumePartition(cfg.Kafka, 0, sarama.OffsetOldest, nil, true) {
		cnt++
		tags[ksample.Sample.Tag] += 1
		progress.Step()
	}
	fmt.Printf("Found %d messages\n", cnt)
	fmt.Println(tags)
}
