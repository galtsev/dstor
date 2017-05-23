package command

import (
	"fmt"
	. "github.com/galtsev/dstor/base"
	"github.com/galtsev/dstor/conf"
	"gopkg.in/yaml.v2"
)

func ShowConfig(args []string) {
	cfg := conf.NewConfig()
	conf.Load(cfg)
	data, err := yaml.Marshal(cfg)
	Check(err)
	fmt.Println(string(data))
}
