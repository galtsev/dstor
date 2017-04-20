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

type Generator interface {
	Next() bool
	Sample() *model.Sample
}

type RandomGenerator struct {
	ts     int64
	end_ts int64
	step   int64
	tags   []string
}

func (g *RandomGenerator) Next() bool {
	g.ts += g.step
	return g.ts < g.end_ts
}

func (g *RandomGenerator) Sample() *model.Sample {
	return MakeSample(g.ts, g.tags[rand.Intn(len(g.tags))])
}

func NewGenerator(cfg GenConfig) Generator {
	ss, ee := cfg.Period()
	ts, end_ts := ss.UnixNano(), ee.UnixNano()
	if end_ts <= ts {
		panic(errors.New("wrong generation period"))
	}
	step := (end_ts - ts) / int64(cfg.Count)
	g := RandomGenerator{
		ts:     ts - step,
		end_ts: end_ts,
		step:   step,
	}
	for i := 0; i < cfg.Tags; i++ {
		g.tags = append(g.tags, fmt.Sprintf("tag%d", i))
	}
	switch cfg.Mode {
	case "random":
		return &g
	case "seq":
		sg := SeqGenerator{
			gen:          &g,
			ts:           g.ts,
			currentTagNo: 0,
			nTags:        cfg.Tags,
		}
		return &sg
	default:
		panic(fmt.Errorf("Wrong generator mode option %s", cfg.Mode))
	}
}

type SeqGenerator struct {
	gen          *RandomGenerator
	ts           int64
	currentTagNo int
	nTags        int
}

func (g *SeqGenerator) Sample() *model.Sample {
	return MakeSample(g.ts, g.gen.tags[g.currentTagNo])
}

func (g *SeqGenerator) Next() bool {
	g.ts += g.gen.step * int64(g.nTags)
	if g.ts < g.gen.end_ts {
		return true
	} else {
		return g.nextTag()
	}
}

func (g *SeqGenerator) nextTag() bool {
	g.currentTagNo++
	if g.currentTagNo < g.nTags {
		g.ts = g.gen.ts
		return true
	}
	return false
}
