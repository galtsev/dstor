package command

import (
	"dan/pimco"
	"dan/pimco/conf"
	"dan/pimco/util"
	"flag"
	"fmt"
)

func Gen(args []string) {
	fs := flag.NewFlagSet("gen", flag.ExitOnError)
	path := fs.String("path", "", "Output file name")
	cfg := conf.LoadConfigEx(fs, args...)
	if *path != "" {
		cfg.FilePath = *path
	}
	fmt.Println(cfg)
	backend := pimco.MakeBackend(cfg.Server.Backend, cfg)
	gen := pimco.NewGenerator(cfg.Gen)
	progress := util.NewProgress(100000)

	for gen.Next() {
		sample := gen.Sample()
		backend.AddSample(sample)
		pimco.SetLatest(sample.TS)
		progress.Step()
	}
	backend.Close()

}
