package main

import (
	"dan/pimco/client"
	"dan/pimco/receptor/batch"
	"dan/pimco/receptor/direct"
	"dan/pimco/receptor/kafka"
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
	default:
		Usage()
	}
}
