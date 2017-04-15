package pimco

import (
	"dan/pimco/model"
	"fmt"
	"math/rand"
)

func MakeSample(ts int64, tag string) model.Sample {
	values := make([]float64, 10)
	v0 := float64(ts / 1000000000)
	for t := range values {
		values[t] = v0 * float64(t+1)
	}
	return model.Sample{
		Tag:    tag,
		Values: values,
		TS:     ts,
	}
}

type Generator struct {
	ts   int64
	step int64
	tags []string
}

func (g *Generator) Next() *model.Sample {
	sample := MakeSample(g.ts, g.tags[rand.Intn(len(g.tags))])
	g.ts += g.step
	return &sample
}

func NewGenerator(tags int, ts, step int64) *Generator {
	g := Generator{
		ts:   ts,
		step: step,
	}
	for i := 0; i < tags; i++ {
		g.tags = append(g.tags, fmt.Sprintf("tag%d", i))
	}
	return &g
}
