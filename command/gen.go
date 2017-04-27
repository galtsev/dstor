package command

import (
	"dan/pimco"
	"dan/pimco/util"
	"fmt"
)

func Gen(args []string) {
	cfg := pimco.LoadConfig(args...)
	fmt.Println(cfg)
	backend := pimco.MakeBackend(cfg.Server.Backend, cfg)
	gen := pimco.NewGenerator(cfg.Gen)
	progress := util.NewProgress(100000)

	for gen.Next() {
		sample := gen.Sample()
		backend.AddSample(sample)
		progress.Step()
	}
	backend.Close()

}
