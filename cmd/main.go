package main

import (
	"dan/pimco/client"
	"dan/pimco/receptor/direct"
	"dan/pimco/receptor/kafka"
	"fmt"
	"os"
)

func Usage() {
	fmt.Printf(`Usage: %s <cmd> <options>
commands: client, recept-direct, recept-kafka`, os.Args[0])
}

func main() {
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
	default:
		Usage()
	}
}
