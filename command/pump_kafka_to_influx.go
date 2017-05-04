/*
	Read messages from Kafka topic and write them to influx using InfluxWriter.
	Read only those messages, which exists in the partition when consumer starts.
*/
package command

import (
	"dan/pimco"
	"dan/pimco/conf"
	"dan/pimco/influx"
	"dan/pimco/kafka"
	"log"
	"sync"
)

func PumpKafka2Influx(args []string) {
	var wg sync.WaitGroup
	cfg := conf.LoadConfig(args...)
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
func consumePartition(cfg conf.Config, partition int32) {
	backend := influx.New(cfg.Influx, pimco.FakeContext{})

	cnt := 0

	for ksample := range kafka.ConsumePartition(cfg.Kafka, partition, cfg.OneShot) {
		backend.AddSample(&ksample.Sample, ksample.Offset)
		cnt++
	}
	backend.Close()
	log.Printf("Partition %d transfered %d samples\n", partition, cnt)
}
