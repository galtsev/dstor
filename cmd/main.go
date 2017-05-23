package main

import (
	"fmt"
	"github.com/galtsev/dstor/command"
	_ "github.com/galtsev/dstor/ldb"
	"log"
	"os"
	"time"
)

func Usage() {
	fmt.Printf(`Usage: %s <cmd> <options>
commands: 
	standalone - start standalone server
	storage-node - start storage node server (cluster mode)
	proxy - start proxy node (cluster mode)

	client - test http client,
	gen - generate samples and write to backend,
	show-config - dump current config to stdout
	topic-stats - read kafka topic, show some stats
	leveldb-stats - show stats for configured local leveldb storage
	query-reporter - get report data from reporter server
	check-report
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
	case "standalone":
		command.Standalone(args)
	case "storage-node":
		command.StorageNode(args)
	case "proxy":
		command.Proxy(args)

	case "client":
		timeIt(func() {
			command.Client(args)
		})
	case "gen":
		timeIt(func() {
			command.Gen(args)
		})
	case "query-reporter":
		timeIt(func() {
			command.QueryReporter(args)
		})
	case "check-report":
		command.CheckReport(args)
	// consume topic from kafka, write to influx
	// generate messages and write to kafka
	case "topic-stats":
		command.TopicStats(args)
	case "show-config":
		command.ShowConfig(args)
	case "leveldb-stats":
		command.LeveldbStats(args)
	default:
		Usage()
	}
}
