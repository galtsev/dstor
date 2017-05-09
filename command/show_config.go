package command

import (
	. "dan/pimco/base"
	"dan/pimco/conf"
	"fmt"
	"gopkg.in/yaml.v2"
)

func ShowConfig(args []string) {
	cfg := conf.NewConfig()
	conf.Load(cfg)
	data, err := yaml.Marshal(cfg)
	Check(err)
	fmt.Println(string(data))
}
