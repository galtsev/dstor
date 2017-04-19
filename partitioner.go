package pimco

import (
	"hash/fnv"
)

func MakePartitioner(topics int) func(string) int32 {
	return func(tag string) int32 {
		h := fnv.New32a()
		h.Write([]byte(tag))
		return int32(h.Sum32()) % int32(topics)
	}
}