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
	conf.Parse(&cfg, args, fs, "gen")
	fmt.Println(cfg)

	storage := injector.MakeStorage(cfg.Gen.Backend, cfg, nil)
	gen := dstor.NewGenerator(cfg.Gen)
	progress := util.NewProgress(100000)

	for gen.Next() {
		sample := gen.Sample()
		storage.AddSample(sample, 0)
		dstor.SetLatest(sample.TS)
		progress.Step()
	}
	storage.Close()

}
