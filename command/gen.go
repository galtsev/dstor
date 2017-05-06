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
	fs := flag.NewFlagSet("gen", flag.ExitOnError)
	path := fs.String("path", "", "Output file name")
	cfg := conf.LoadConfigEx(fs, args...)
	if *path != "" {
		cfg.FilePath = *path
	}
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
