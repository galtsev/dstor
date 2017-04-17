package command

import (
	"dan/pimco"
	. "dan/pimco/base"
	"fmt"
	"gopkg.in/yaml.v2"
)

func ShowConfig(args []string) {
	config := pimco.LoadConfig(args...)
	data, err := yaml.Marshal(config)
	Check(err)
	fmt.Println(string(data))
}
