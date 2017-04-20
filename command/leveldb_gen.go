package command

import (
	"dan/pimco"
	"dan/pimco/util"
	"log"
)

func LeveldbGen(args []string) {
	cfg := pimco.LoadConfig(args...)
	log.Println(cfg)
	srv := NewStandaloneLeveldbServer(cfg)
	gen := pimco.NewGenerator(cfg.Gen)
	progress := util.NewProgress(100000)
	for gen.Next() {
		srv.WriteSample(gen.Sample())
		progress.Step()
	}
	srv.Close()
}
