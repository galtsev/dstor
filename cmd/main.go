package main

import (
	"dan/pimco/client"
	"dan/pimco/loader"
	"dan/pimco/receptor/batch"
	"dan/pimco/receptor/direct"
	"dan/pimco/receptor/kafka"
	"dan/pimco/temp"
	"dan/pimco/temp/gen1"
	"dan/pimco/temp/gen2"
	"fmt"
	"log"
	"os"
)

func Usage() {
	fmt.Printf(`Usage: %s <cmd> <options>
commands: client, recept-direct, recept-kafka`, os.Args[0])
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
	// temporary
	case "gen1":
		temp.TimeIt(func() {
			gen1.Run(args)
		})
	case "gen2":
		temp.TimeIt(func() {
			gen2.Run(args)
		})
	default:
		Usage()
	}
}
