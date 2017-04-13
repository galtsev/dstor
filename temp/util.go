package temp

import (
	"dan/pimco/model"
	"fmt"
	"time"
)

func TimeIt(fn func()) {
	start := time.Now()
	fn()
	fmt.Printf("Duration: %v\n", time.Since(start))
}

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
