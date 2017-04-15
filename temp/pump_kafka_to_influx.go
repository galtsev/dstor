/*
	Read messages from Kafka topic and write them to influx using InfluxWriter.
	Read only those messages, which exists in the partition when consumer starts.
*/
package temp

import (
	"dan/pimco"
	"dan/pimco/influx"
	"dan/pimco/kafka"
	"fmt"
	"time"
)

func PumpKafka2Influx(args []string) {
	cfg := pimco.LoadConfig(args...)
	fmt.Println(cfg)
	fluxWriter := influx.NewWriter(cfg.Influx)
	w := pimco.NewWriter(fluxWriter, cfg.Influx.BatchSize, time.Duration(cfg.Influx.FlushDelay)*time.Millisecond)

	cnt := 0

	for sample := range kafka.ConsumePartition(cfg.Kafka, cfg.Kafka.Partitions[0], true) {
		w.Write(&sample)
		cnt++
	}
	w.Close()
	fmt.Printf("transfered %d samples\n", cnt)

}
