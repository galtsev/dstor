package main

import (
	"dan/pimco/command"
	"fmt"
	"log"
	"os"
	"time"
)

func Usage() {
	fmt.Printf(`Usage: %s <cmd> <options>
commands: 
	client - test http client, 
	reporting-server-mem - reporting server with embedded loader. Read Kafka continuously, keep all samples in memory.
	show-config - dump current config to stdout
	gen2flux - generate samples and write directly to influxdb
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
		//client.Run(args)
		timeIt(func() {
			command.Client(args)
		})
	case "query-flux":
		timeIt(func() { command.QueryFlux(args) })
		//command.QueryFlux(args)
	case "recept":
		command.Recept2Kafka(args)
	case "reporting-server-mem":
		command.MemServer(args)
	case "gen2flux":
		timeIt(func() {
			command.Run2(args)
		})
	// consume topic from kafka, write to influx
	case "kafka2flux":
		timeIt(func() {
			command.PumpKafka2Influx(args)
		})
	// generate messages and write to kafka
	case "gen2kafka":
		timeIt(func() {
			command.Run4(args)
		})
	case "topic-stats":
		command.TopicStats(args)
	case "show-config":
		command.ShowConfig(args)
	default:
		Usage()
	}
}
