package command

import (
	. "dan/pimco/base"
	"dan/pimco/conf"
	"fmt"
	"gopkg.in/yaml.v2"
)

func ShowConfig(args []string) {
	config := conf.LoadConfig(args...)
	data, err := yaml.Marshal(config)
	Check(err)
	fmt.Println(string(data))
}
