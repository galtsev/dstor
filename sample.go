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
	ts     int64
	end_ts int64
	step   int64
	tags   []string
}

func (g *Generator) Next() bool {
	g.ts += g.step
	return g.ts <= g.end_ts
}

func (g *Generator) Sample() *model.Sample {
	sample := MakeSample(g.ts, g.tags[rand.Intn(len(g.tags))])
	g.ts += g.step
	return &sample
}

func NewGenerator(cfg GenConfig) *Generator {
	ss, ee := cfg.Period()
	ts, end_ts := ss.UnixNano(), ee.UnixNano()
	step := (end_ts - ts) / int64(cfg.Count)
	g := Generator{
		ts:     ts - 1,
		end_ts: end_ts,
		step:   step,
	}
	for i := 0; i < cfg.Tags; i++ {
		g.tags = append(g.tags, fmt.Sprintf("tag%d", i))
	}
	return &g
}
