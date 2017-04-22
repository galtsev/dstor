package pimco

import (
	"dan/pimco/model"
	"errors"
	"fmt"
	"math"
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

type BaseGenerator struct {
	start int64
	end   int64
	step  int64
	tags  []string
}

type RandomGenerator struct {
	BaseGenerator
	ts int64
}

func (g *RandomGenerator) Next() bool {
	g.ts += g.step
	return g.ts < g.end
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
	base := BaseGenerator{
		start: ts,
		end:   end_ts,
		step:  step,
	}
	for i := 0; i < cfg.Tags; i++ {
		base.tags = append(base.tags, fmt.Sprintf("tag%d", i))
	}
	switch cfg.Mode {
	case "random":
		return &RandomGenerator{
			BaseGenerator: base,
			ts:            base.start - base.step,
		}
	case "seq":
		return &SeqGenerator{
			BaseGenerator: base,
			ts:            base.start - base.step,
			currentTagNo:  0,
		}
	case "geom":
		return &GeomRandomGenerator{
			BaseGenerator: base,
			ts:            base.start - base.step,
			pp:            1 / math.Log(1-0.01),
		}
	case "geom-seq":
		g := &GeomSeqGenerator{
			BaseGenerator: base,
			ts:            base.start - base.step,
			currentTagNo:  -1,
		}
		g.nextTag()
		return g
	default:
		panic(fmt.Errorf("Wrong generator mode option %s", cfg.Mode))
	}
}

/* generate samples sequentially, for test batch load
   samples distributed uniformly between tags (same number of samples for each tag)
*/
type SeqGenerator struct {
	BaseGenerator
	ts           int64
	currentTagNo int
}

func (g *SeqGenerator) Sample() *model.Sample {
	return MakeSample(g.ts, g.tags[g.currentTagNo])
}

func (g *SeqGenerator) Next() bool {
	g.ts += g.step * int64(len(g.tags))
	if g.ts < g.end {
		return true
	} else {
		return g.nextTag()
	}
}

func (g *SeqGenerator) nextTag() bool {
	g.currentTagNo++
	if g.currentTagNo < len(g.tags) {
		g.ts = g.start
		return true
	}
	return false
}

/* generate samples sequentially, for test batch load
   samples distributed geometrically between tags (in a sequence of tags each tag provide 1% of remaining number of samples)
*/
type GeomSeqGenerator struct {
	BaseGenerator
	ts            int64
	samplesRemain int
	step          int64
	currentTagNo  int
}

func (g *GeomSeqGenerator) Sample() *model.Sample {
	return MakeSample(g.ts, g.tags[g.currentTagNo])
}

func (g *GeomSeqGenerator) Next() bool {
	g.ts += g.step
	if g.ts < g.end {
		return true
	} else {
		return g.nextTag()
	}
}

func (g *GeomSeqGenerator) nextTag() bool {
	g.currentTagNo++
	if g.currentTagNo < len(g.tags) {
		g.ts = g.start
		var samplesToGenerate int
		if g.samplesRemain > 101 {
			samplesToGenerate = g.samplesRemain / 100
		} else {
			samplesToGenerate = g.samplesRemain
		}
		g.samplesRemain -= samplesToGenerate
		g.step = (g.end - g.start) / int64(samplesToGenerate)
		return true
	} else {
		return false
	}
}

type GeomRandomGenerator struct {
	BaseGenerator
	ts int64
	pp float64
}

func (g *GeomRandomGenerator) chooseTag() string {
	for {
		v := rand.Float64()
		if v == 0.0 {
			continue
		}
		i := int(g.pp * math.Log(v))
		if i < len(g.tags) {
			return g.tags[i]
		}
	}
}

func (g *GeomRandomGenerator) Sample() *model.Sample {
	return MakeSample(g.ts, g.chooseTag())
}

func (g *GeomRandomGenerator) Next() bool {
	g.ts += g.step
	return g.ts < g.end
}
