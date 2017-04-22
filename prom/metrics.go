package prom

import (
	"github.com/prometheus/client_golang/prometheus"
	"time"
)

const ()

var (
	defaultPercentiles = map[float64]float64{
		0.5:  0.05,
		0.9:  0.01,
		0.99: 0.001,
	}

	sampleWriteHist = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "pimco_sample_write_hist",
		Help:    "Delay between sample accepted from client and became available for reports, seconds",
		Buckets: []float64{0.001, 0.01, 0.1, 1.0},
	})
	sampleWriteSum = prometheus.NewSummary(prometheus.SummaryOpts{
		Name:       "pimco_sample_write_sum",
		Help:       "Delay between sample accepted from client and became available for reports, percentiles",
		MaxAge:     time.Second,
		Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
	})
	requestTimeHist = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "pimco_request_time_hist",
		Help:    "request serve time per path, seconds",
		Buckets: []float64{0.001, 0.002, 0.005, 0.01, 0.1},
	}, []string{"path"})
	requestTimeSum = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Name:       "pimco_request_time_sum",
		Help:       "request serve time per path, percentiles",
		MaxAge:     time.Second,
		Objectives: defaultPercentiles,
	}, []string{"path"})
	enableHist bool
	enableSum  bool
)

func Setup(hist, sum bool) {
	prometheus.MustRegister(
		sampleWriteHist,
		sampleWriteSum,
		requestTimeHist,
		requestTimeSum,
	)
	enableHist = hist
	enableSum = sum
}

func SampleWrite(duration time.Duration) {
	p := float64(duration) / (1000 * 1000 * 1000)
	if enableHist {
		sampleWriteHist.Observe(p)
	}
	if enableSum {
		sampleWriteSum.Observe(p)
	}
}

func RequestTime(path string, duration time.Duration) {
	p := float64(duration) / (1000 * 1000 * 1000)
	if enableHist {
		requestTimeHist.WithLabelValues(path).Observe(p)
	}
	if enableSum {
		requestTimeSum.WithLabelValues(path).Observe(p)
	}
}
