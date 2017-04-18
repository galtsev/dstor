package command

import (
	"dan/pimco"
	"dan/pimco/phttp"
	"dan/pimco/skipdb"
	"log"
)

func MemServer(args []string) {
	cfg := pimco.LoadConfig(args...)
	log.Println(cfg)
	db := skipdb.NewDB()
	phttp.Serve(cfg, db)
}
