package command

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/galtsev/dstor/conf"
	"github.com/galtsev/dstor/kafka"
	"github.com/galtsev/dstor/util"
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
