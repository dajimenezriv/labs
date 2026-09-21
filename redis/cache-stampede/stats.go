package main

import (
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// stats is everything the report needs, behind one mutex, plus the same
// numbers as Prometheus metrics for the dashboard. Every request already
// spends either a Redis round trip or the whole origin latency in here; a lock
// held for the length of a map write does not move any number in this lab.
type stats struct {
	mu sync.Mutex

	frozen bool
	hits   int
	misses int
	loads  int
	// Origin loads running right now, and the most that ever ran at once. The
	// peak is the number the origin actually feels: a thousand loads spread
	// over a run is nothing, and four hundred at one instant is what exhausts
	// a connection pool.
	inflight int
	peak     int
	lat      []time.Duration

	reqs   *prometheus.CounterVec
	origin prometheus.Counter
	hist   prometheus.Histogram
}

func newStats() *stats {
	return &stats{
		reqs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cache_requests_total",
			Help: "Application requests for the hot key, by cache result.",
		}, []string{"result"}),
		origin: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "cache_origin_loads_total",
			Help: "Recomputes sent to the origin. In cache-aside this equals misses.",
		}),
		hist: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "cache_request_duration_seconds",
			Help:    "End to end request latency, hits and misses together.",
			Buckets: prometheus.ExponentialBuckets(0.0001, 2.5, 14),
		}),
	}
}

func (s *stats) register(reg *prometheus.Registry) {
	reg.MustRegister(s.reqs, s.origin, s.hist)
}

func (s *stats) hit() {
	s.reqs.WithLabelValues("hit").Inc()
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.frozen {
		s.hits++
	}
}

func (s *stats) miss() {
	s.reqs.WithLabelValues("miss").Inc()
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.frozen {
		s.misses++
	}
}

func (s *stats) originStart() {
	s.origin.Inc()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inflight++
	if s.frozen {
		return
	}
	s.loads++
	s.peak = max(s.peak, s.inflight)
}

func (s *stats) originEnd() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inflight--
}

func (s *stats) latency(d time.Duration) {
	s.hist.Observe(d.Seconds())
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.frozen {
		s.lat = append(s.lat, d)
	}
}

// reset drops what the warm-up produced, so the run starts from a key that is
// already cached and every herd counted below is a TTL expiry.
func (s *stats) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hits, s.misses, s.loads, s.peak = 0, 0, 0, 0
	s.lat = nil
}

// freeze stops counting. The generator waits for every request it dispatched
// before this is called, so the last herd is fully measured and there is
// nothing in flight to cut off; this only keeps the metrics endpoint, which is
// still up until the process exits, from adding to a report already printed.
func (s *stats) freeze() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.frozen = true
}

func (s *stats) percentile(p float64) time.Duration {
	if len(s.lat) == 0 {
		return 0
	}
	i := int(p * float64(len(s.lat)-1))
	return s.lat[i]
}

func printHeader() {
	fmt.Println("variant\t| origin\t| requests\t| hit %\t| origin loads\t| peak concurrent\t| p50\t| p99\t| max")
}

func (s *stats) printRow(cfg config) {
	s.mu.Lock()
	defer s.mu.Unlock()
	slices.Sort(s.lat)

	total := s.hits + s.misses
	hitPct := 0.0
	if total > 0 {
		hitPct = 100 * float64(s.hits) / float64(total)
	}
	fmt.Printf("%s\t| %s\t| %d\t| %.2f\t| %d\t| %d\t| %s\t| %s\t| %s\n",
		cfg.label, cfg.origin, total, hitPct, s.loads, s.peak,
		round(s.percentile(0.50)), round(s.percentile(0.99)), round(s.percentile(1)))
}

// round keeps the table readable: a hit is measured in microseconds and a
// request caught in a herd in hundreds of milliseconds, and both have to fit
// in the same column.
func round(d time.Duration) time.Duration {
	switch {
	case d < time.Millisecond:
		return d.Round(10 * time.Microsecond)
	case d < time.Second:
		return d.Round(time.Millisecond)
	default:
		return d.Round(10 * time.Millisecond)
	}
}
