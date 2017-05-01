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
	inj := injector.New(cfg)
	storage := inj.Storage()
	gen := inj.Generator()
	progress := util.NewProgress(100000)

	for gen.Next() {
		sample := gen.Sample()
		storage.AddSample(sample)
		pimco.SetLatest(sample.TS)
		progress.Step()
	}
	storage.Close()

}
