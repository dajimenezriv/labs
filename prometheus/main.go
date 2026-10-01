package main

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var reqDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Name:    "http_request_duration_seconds",
	Help:    "HTTP request latency.",
	Buckets: prometheus.DefBuckets,
}, []string{"route", "method", "code"})

type recorder struct {
	http.ResponseWriter
	code int
}

func (r *recorder) WriteHeader(c int) {
	r.code = c
	r.ResponseWriter.WriteHeader(c)
}

func instrument(route string, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rec := &recorder{w, 200}
		start := time.Now()
		h(rec, r)
		reqDuration.WithLabelValues(route, r.Method, strconv.Itoa(rec.code)).Observe(time.Since(start).Seconds())
	}
}

func main() {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.Handle("GET /users/{id}", instrument("/users/{id}", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(5 * time.Millisecond)
	}))
	http.ListenAndServe(":8000", mux)
}
