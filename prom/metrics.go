package prom

import (
	"github.com/galtsev/dstor/conf"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"log"
	"net/http"
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
	cfg conf.MetricsConfig
)

func Setup(metricsConfig conf.MetricsConfig) {
	cfg = metricsConfig
	prometheus.MustRegister(
		sampleWriteHist,
		sampleWriteSum,
		requestTimeHist,
		requestTimeSum,
	)
	http.Handle("/metrics", promhttp.Handler())
	go func() {
		log.Fatalln(http.ListenAndServe(cfg.Addr, nil))
	}()
}

func SampleWrite(duration time.Duration) {
	p := float64(duration) / (1000 * 1000 * 1000)
	if cfg.EnableHist {
		sampleWriteHist.Observe(p)
	}
	if cfg.EnableSum {
		sampleWriteSum.Observe(p)
	}
}

func RequestTime(path string, duration time.Duration) {
	p := float64(duration) / (1000 * 1000 * 1000)
	if cfg.EnableHist {
		requestTimeHist.WithLabelValues(path).Observe(p)
	}
	if cfg.EnableSum {
		requestTimeSum.WithLabelValues(path).Observe(p)
	}
}
