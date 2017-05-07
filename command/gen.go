package command

import (
	"dan/pimco"
	"dan/pimco/conf"
	"dan/pimco/injector"
	"dan/pimco/util"
	"flag"
	"fmt"
)

func Gen(args []string) {
	var cfg conf.Config
	conf.Load(&cfg, args...)
	fs := flag.NewFlagSet("gen", flag.ExitOnError)
	fs.StringVar(&cfg.Gen.Backend, "backend", cfg.Gen.Backend, "backend")
	fs.Parse(args)
	fmt.Println(cfg)
	storage := injector.MakeStorage(cfg.Gen.Backend, cfg, nil)
	gen := pimco.NewGenerator(cfg.Gen)
	progress := util.NewProgress(100000)

	for gen.Next() {
		sample := gen.Sample()
		storage.AddSample(sample, 0)
		pimco.SetLatest(sample.TS)
		progress.Step()
	}
	storage.Close()

}
