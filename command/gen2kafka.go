/*
	Generate messages and write them all to Kafka topic, partition 0
*/
package command

import (
	"dan/pimco"
	"dan/pimco/kafka"
	"fmt"
	"time"
)

func Run4(args []string) {
	cfg := pimco.LoadConfig(args...)
	fmt.Println(cfg)
	kafkaWriter := kafka.NewWriter(cfg.Kafka, int32(0))
	w := pimco.NewWriter(kafkaWriter, cfg.Kafka.BatchSize, time.Duration(cfg.Kafka.FlushDelay)*time.Millisecond)
	gen := pimco.NewGenerator(cfg.Gen)
	for gen.Next() {
		w.Write(gen.Sample())
	}
	w.Close()

}
