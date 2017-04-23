package main

import (
	"dan/pimco/command"
	_ "dan/pimco/ldb"
	"fmt"
	"log"
	"os"
	"time"
)

func Usage() {
	fmt.Printf(`Usage: %s <cmd> <options>
commands: 
	client - test http client,
	gen - generate samples and write to backend,
	serve - start http server that handle /write and /report requests using specified backend
	receipt - start http server that handle /write requests and write to kafka
	show-config - dump current config to stdout
	kafka2flux - read from kafka, write to influxdb
	topic-stats - read kafka topic, show some stats
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
	case "gen":
		timeIt(func() {
			command.Gen(args)
		})
	case "serve":
		command.Serve(args)
		//command.QueryFlux(args)
	case "query-reporter":
		timeIt(func() { command.QueryReporter(args) })
	case "recept":
		command.Recept2Kafka(args)
	// consume topic from kafka, write to influx
	case "kafka2flux":
		timeIt(func() {
			command.PumpKafka2Influx(args)
		})
	// generate messages and write to kafka
	case "topic-stats":
		command.TopicStats(args)
	case "show-config":
		command.ShowConfig(args)
	default:
		Usage()
	}
}
