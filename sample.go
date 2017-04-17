package pimco

import (
	"dan/pimco/model"
	"errors"
	"fmt"
	"math/rand"
)

func MakeSample(ts int64, tag string) *model.Sample {
	values := make([]float64, 10)
	v0 := float64(ts / 1000000000)
	for t := range values {
		values[t] = v0 * float64(t+1)
	}
	sample := model.Sample{
		Tag: tag,
		TS:  ts,
	}
	for i := range sample.Values {
		sample.Values[i] = v0 * float64(i+1)
	}
	return &sample
}

type Generator struct {
	ts     int64
	end_ts int64
	step   int64
	tags   []string
}

func (g *Generator) Next() bool {
	g.ts += g.step
	return g.ts < g.end_ts
}

func (g *Generator) Sample() *model.Sample {
	return MakeSample(g.ts, g.tags[rand.Intn(len(g.tags))])
}

func NewGenerator(cfg GenConfig) *Generator {
	ss, ee := cfg.Period()
	ts, end_ts := ss.UnixNano(), ee.UnixNano()
	if end_ts <= ts {
		panic(errors.New("wrong generation period"))
	}
	step := (end_ts - ts) / int64(cfg.Count)
	g := Generator{
		ts:     ts - step,
		end_ts: end_ts,
		step:   step,
	}
	for i := 0; i < cfg.Tags; i++ {
		g.tags = append(g.tags, fmt.Sprintf("tag%d", i))
	}
	return &g
}
