package command

import (
	"flag"
	"fmt"
	"github.com/galtsev/dstor"
	"github.com/galtsev/dstor/conf"
	"github.com/galtsev/dstor/injector"
	"github.com/galtsev/dstor/util"
)

func Gen(args []string) {
	cfg := *conf.NewConfig()
	conf.Load(&cfg)
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
