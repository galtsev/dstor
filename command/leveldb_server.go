package command

import (
	"dan/pimco"
	"dan/pimco/ldb"
	"dan/pimco/phttp"
	"log"
)

func LeveldbServer(args []string) {
	cfg := pimco.LoadConfig(args...)
	log.Println(cfg)
	db := ldb.Open(cfg.Leveldb, int32(0))
	phttp.Serve(cfg, db)
}
