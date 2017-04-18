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
	reporting-server-leveldb - reporting server that use leveldb as backend storage
	reporting-server-influx - serve reports from influxdb
	show-config - dump current config to stdout
	gen2flux - generate samples and write directly to influxdb
	gen2kafka - generate samples and write to kafka
	gen2leveldb - gen directly to leveldb
	kafka2flux - read from kafka, write to influxdb
	topic-stats - read kafka topic, show some stats
	query-flux - get report data directly from influxdb
	query-reporter - get report data from reporter server
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
	case "query-reporter":
		timeIt(func() { command.QueryReporter(args) })
	case "recept":
		command.Recept2Kafka(args)
	case "reporting-server-leveldb":
		command.LeveldbServer(args)
	case "reporting-server-mem":
		command.MemServer(args)
	case "reporting-server-influx":
		command.FluxServer(args)
	case "gen2flux":
		timeIt(func() {
			command.Run2(args)
		})
	case "gen2leveldb":
		timeIt(func() { command.Gen2Leveldb(args) })
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
