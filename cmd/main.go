package main

import (
	"dan/pimco/client"
	"dan/pimco/loader"
	"dan/pimco/receptor/batch"
	"dan/pimco/receptor/direct"
	"dan/pimco/receptor/kafka"
	"dan/pimco/temp"
	"fmt"
	"log"
	"os"
	"time"
)

func Usage() {
	fmt.Printf(`Usage: %s <cmd> <options>
commands: 
	client - test http client, 
	recept-direct - http server that write samples directly to influxdb,
	recept-kafka - http server that write samples to kafka using async producer (one sample per kafka message)
	recept-batch - http server that write samples to kafka (many samples per kafka message)
	loader - pump messages from kafka to influxdb

	# Reporting servers
	reporting-server-mem - reporting server with embedded loader. Read Kafka continuously, keep all samples in memory.

	show-config - dump current config to stdout

	# experimenting
	gen1 - ?
	gen2flus - generate samples and write directly to influxdb
	gen2kafka - generate samples and write to kafka
	kafka2flux - read from kafka, write to influxdb
	topic-stats - read kafka topic, show some stats
`, os.Args[0])
}

func timeIt(fn func()) {
	start := time.Now()
	fn()
	fmt.Printf("Duration: %v\n", time.Since(start))
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	if len(os.Args) < 2 {
		Usage()
		return
	}
	cmd := os.Args[1]
	args := os.Args[2:]
	switch cmd {
	case "client":
		client.Run(args)
	case "recept-direct":
		direct.Run(args)
	case "recept-kafka":
		kafka.Run(args)
	case "recept-batch":
		batch.Run(args)
	case "loader":
		loader.Run(args)
	case "reporting-server-mem":
		temp.MemServer(args)
	// temporary
	case "gen2flux":
		timeIt(func() {
			temp.Run2(args)
		})
	// consume topic from kafka, write to influx
	case "kafka2flux":
		timeIt(func() {
			temp.PumpKafka2Influx(args)
		})
	// generate messages and write to kafka
	case "gen2kafka":
		timeIt(func() {
			temp.Run4(args)
		})
	case "topic-stats":
		temp.TopicStats(args)
	case "show-config":
		temp.ShowConfig(args)
	default:
		Usage()
	}
}
