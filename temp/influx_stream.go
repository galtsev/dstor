package temp

import (
	"dan/pimco/model"
	. "dan/pimco/util"
	"github.com/influxdata/influxdb/client/v2"
	"log"
	"sync"
	"time"
)

type InfluxStream struct {
	In   chan model.Sample
	conn client.Client
	cfg  InfluxConfig
	wg   sync.WaitGroup
}

func NewInfluxStream(cfg InfluxConfig) *InfluxStream {
	stream := InfluxStream{
		In:  make(chan model.Sample),
		cfg: cfg,
	}
	conn, err := client.NewHTTPClient(client.HTTPConfig{Addr: cfg.URL})
	Check(err)
	stream.conn = conn
	go writeLoop(&stream)
	return &stream
}

func (stream *InfluxStream) Close() {
	stream.wg.Add(1)
	close(stream.In)
	stream.wg.Wait()
}

func writeLoop(stream *InfluxStream) {
	bpConfig := client.BatchPointsConfig{Database: stream.cfg.Database}
	var batch client.BatchPoints
	var err error
	var cnt int
	var chanClosed bool
	flushDelay := time.Duration(stream.cfg.FlushDelay) * time.Millisecond
	timer := time.NewTimer(flushDelay)
	for !chanClosed {
		// reset to initial state (empty batch)
		batch, err = client.NewBatchPoints(bpConfig)
		Check(err)
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		cnt = 0

		// wait for first sample or channel close, no timeout
		sample, ok := <-stream.In
		if !ok {
			// channel closed, no samples collected yet, just exit
			break
		}
		AddSample(&sample, batch)
		cnt++
		timer.Reset(flushDelay)
		flush := false
		for !flush {
			select {
			case sample, ok := <-stream.In:
				if ok {
					AddSample(&sample, batch)
					cnt++
					if cnt >= stream.cfg.BatchSize {
						flush = true
					}
				} else {
					flush = true
					chanClosed = true
				}
			case <-timer.C:
				flush = true
			}
		}
		stream.conn.Write(batch)
		log.Printf("Writing batch of size %d", cnt)
	}
	stream.wg.Done()
}
