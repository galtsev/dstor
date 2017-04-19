package command

import (
	"dan/pimco"
	"dan/pimco/kafka"
	"fmt"
)

func TopicStats(args []string) {
	cfg := pimco.LoadConfig(args...)
	fmt.Println(cfg)
	cnt := 0
	tags := make(map[string]int)
	for sample := range kafka.ConsumePartition(cfg.Kafka, 0, true) {
		cnt++
		tags[sample.Tag] += 1
	}
	fmt.Printf("Found %d messages\n", cnt)
	fmt.Println(tags)
}