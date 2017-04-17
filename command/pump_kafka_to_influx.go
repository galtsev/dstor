/*
	Read messages from Kafka topic and write them to influx using InfluxWriter.
	Read only those messages, which exists in the partition when consumer starts.
*/
package command

import (
	"dan/pimco"
	"dan/pimco/influx"
	"dan/pimco/kafka"
	"log"
	"sync"
	"time"
)

func PumpKafka2Influx(args []string) {
	var wg sync.WaitGroup
	cfg := pimco.LoadConfig(args...)
	log.Println(cfg)
	for _, partition := range cfg.Kafka.Partitions {
		wg.Add(1)
		go func(p int32) {
			consumePartition(cfg, p)
			wg.Done()
		}(partition)
	}
	wg.Wait()
}

// TODO Graceful cancelation
func consumePartition(cfg pimco.Config, partition int32) {
	fluxWriter := influx.NewWriter(cfg.Influx)
	w := pimco.NewWriter(fluxWriter, cfg.Influx.BatchSize, time.Duration(cfg.Influx.FlushDelay)*time.Millisecond)

	cnt := 0

	for sample := range kafka.ConsumePartition(cfg.Kafka, partition, cfg.OneShot) {
		w.Write(&sample)
		cnt++
	}
	w.Close()
	log.Printf("Partition %d transfered %d samples\n", partition, cnt)
}
