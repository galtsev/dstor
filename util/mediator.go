package util

import (
	"time"
)

/* Send ch <- rate*resolution/time.Second every resolution until *duration passed.
   if duration==nil, run indefinitely
*/
func Mediate(ch chan int, rate int, resolution time.Duration, duration *time.Duration) {
	t := time.NewTicker(resolution)
	defer t.Stop()
	var finish time.Time
	if duration != nil {
		finish = time.Now().Add(*duration)
	}
	for {
		<-t.C
		ch <- rate * int(resolution/time.Second)
		if !finish.IsZero() && time.Now().After(finish) {
			break
		}
	}
}
