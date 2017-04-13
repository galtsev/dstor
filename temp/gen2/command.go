package gen2

import (
	"dan/pimco/temp"
	. "dan/pimco/util"
	"fmt"
	"math/rand"
	"time"
)

const (
	start = "2017-04-06 10:00"
	step  = 400 //ms
	// Mon Jan 2 15:04:05 -0700 MST 2006
	date_format            = "2006-01-02 15:04"
	daySeconds     float64 = 86400
	sentDelayAfter         = 700 //messages
	sentDelayMs            = 60
)

func Run(args []string) {
	cfg := temp.LoadConfig(args...)
	fmt.Println(cfg)
	tags := make([]string, cfg.Tags)
	for i := range tags {
		tags[i] = fmt.Sprintf("tag_%d", i)
	}
	stream := temp.NewInfluxStream(cfg.Influx)
	dt, err := time.Parse(date_format, start)
	Check(err)
	cnt := 0
	for i := 0; i < cfg.Count; i++ {
		ts := dt.UnixNano()
		stream.In <- temp.MakeSample(ts, tags[rand.Intn(len(tags))])
		dt = dt.Add(step * time.Millisecond)
		cnt++
		if cnt >= sentDelayAfter {
			timer := time.NewTimer(time.Duration(sentDelayMs) * time.Millisecond)
			<-timer.C
			cnt = 0
		}
	}
	fmt.Println("waiting for stream.close")
	stream.Close()

}
