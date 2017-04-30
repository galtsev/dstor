/*
	Generate messages and write them all to Kafka topic, partition 0
*/
package command

import (
	"dan/pimco"
	"dan/pimco/conf"
	"dan/pimco/kafka"
	"dan/pimco/util"
	"fmt"
)

func Run4(args []string) {
	cfg := conf.LoadConfig(args...)
	fmt.Println(cfg)
	kafkaWriter := kafka.NewWriter(cfg.Kafka, int32(0))
	w := pimco.NewWriter(kafkaWriter, cfg.Batch)
	gen := pimco.NewGenerator(cfg.Gen)
	progress := util.NewProgress(100000)
	for gen.Next() {
		w.Write(gen.Sample())
		progress.Step()
	}
	w.Close()

}
