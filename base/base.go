package base

import (
	"dan/pimco/model"
	"github.com/influxdata/influxdb/client/v2"
	"time"
)

var (
	FIELD_NAMES = []string{"V1", "V2", "V3", "V4", "V5", "V6", "V7", "V8", "V9", "V10"}
	EMPTY_TAGS  = map[string]string{}
)

func Check(err error) {
	if err != nil {
		panic(err)
	}
}

func AddSample(sample *model.Sample, batch client.BatchPoints) {
	fields := make(map[string]interface{})
	for idx, fn := range FIELD_NAMES {
		fields[fn] = sample.Values[idx]
	}
	point, _ := client.NewPoint("ms", map[string]string{"tag": sample.Tag}, fields, time.Unix(0, sample.TS))
	batch.AddPoint(point)
}

func AddSample_OLD(sample *model.Sample, batch client.BatchPoints) {
	fields := make(map[string]interface{})
	for idx, fn := range FIELD_NAMES {
		fields[fn] = sample.Values[idx]
	}
	point, _ := client.NewPoint(sample.Tag, EMPTY_TAGS, fields, time.Unix(0, sample.TS))
	batch.AddPoint(point)
}
