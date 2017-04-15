package influx

import (
	"dan/pimco"
	. "dan/pimco/base"
	"dan/pimco/model"
	"github.com/influxdata/influxdb/client/v2"
)

type Writer struct {
	conn     client.Client
	cfg      pimco.InfluxConfig
	bpConfig client.BatchPointsConfig
	batch    client.BatchPoints
}

func NewWriter(cfg pimco.InfluxConfig) *Writer {
	w := Writer{
		cfg:      cfg,
		bpConfig: client.BatchPointsConfig{Database: cfg.Database},
	}
	conn, err := client.NewHTTPClient(client.HTTPConfig{Addr: cfg.URL})
	Check(err)
	w.conn = conn
	return &w
}

func (w *Writer) Add(sample *model.Sample) {
	if w.batch == nil {
		batch, err := client.NewBatchPoints(w.bpConfig)
		Check(err)
		w.batch = batch
	}
	AddSample(sample, w.batch)
}

func (w *Writer) Flush() {
	if w.batch != nil {
		Check(w.conn.Write(w.batch))
		w.batch = nil
	}
}

func (w *Writer) Close() {
	w.Flush()
	w.conn.Close()
}
