package pimco

import (
	. "dan/pimco"
	"dan/pimco/conf"
	"dan/pimco/model"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestGenerator(t *testing.T) {
	cfg := conf.GenConfig{
		Start: "2017-04-17 08:30",
		End:   "2017-04-17 9:30",
		Count: 1800,
		Tags:  20,
	}
	gen := NewGenerator(cfg)
	expectedFirst, err := time.Parse(DATE_FORMAT, cfg.Start)
	assert.NoError(t, err)
	gen.Next()
	first := gen.Sample()
	assert.Equal(t, expectedFirst.UnixNano(), first.TS)
	cnt := 1
	var sample *model.Sample
	for gen.Next() {
		sample = gen.Sample()
		cnt++
	}
	assert.Equal(t, cfg.Count, cnt)

	expectedLast, err := time.Parse(DATE_FORMAT, cfg.End)
	assert.NoError(t, err)
	expectedLast = expectedLast.Add(time.Duration(-2) * time.Second)
	assert.Equal(t, expectedLast.UnixNano(), sample.TS)
}
