/*
	Generate messages and write them all to Kafka topic, partition 0
*/
package temp

import (
	"dan/pimco"
	. "dan/pimco/base"
	"dan/pimco/kafka"
	"fmt"
	"time"
)

func Run4(args []string) {
	cfg := pimco.LoadConfig(args...)
	fmt.Println(cfg)
	kafkaWriter := kafka.NewWriter(cfg.Kafka, int32(0))
	w := pimco.NewWriter(kafkaWriter, cfg.Kafka.BatchSize, time.Duration(cfg.Kafka.FlushDelay)*time.Millisecond)
	dt, err := time.Parse(date_format, cfg.Gen.Start)
	Check(err)
	gen := pimco.NewGenerator(cfg.Gen.Tags, dt.UnixNano(), cfg.Gen.Step)
	for i := 0; i < cfg.Gen.Count; i++ {
		w.Write(gen.Next())
	}
	w.Close()

}
