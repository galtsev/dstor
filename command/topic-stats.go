package command

import (
	"dan/pimco/conf"
	"dan/pimco/kafka"
	"fmt"
	"github.com/Shopify/sarama"
)

func TopicStats(args []string) {
	cfg := conf.LoadConfig(args...)
	fmt.Println(cfg)
	cnt := 0
	tags := make(map[string]int)
	for ksample := range kafka.ConsumePartition(cfg.Kafka, 0, sarama.OffsetOldest, true) {
		cnt++
		tags[ksample.Sample.Tag] += 1
	}
	fmt.Printf("Found %d messages\n", cnt)
	fmt.Println(tags)
}
