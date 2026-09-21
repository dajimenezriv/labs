package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// stats is everything the report needs, behind one mutex, plus the same
// numbers as Prometheus metrics for the dashboard. Every counter here is a
// count of something that should be impossible: two workers inside one
// critical section, a release that freed somebody else's lock, a write from a
// worker that stopped being the holder while it worked.
type stats struct {
	mu sync.Mutex

	frozen   bool
	runs     int
	overlaps int
	stolens  int
	stales   int
	corrupts int
	rejects  int
	aborts   int
	start    time.Time

	acquires *prometheus.CounterVec
	writes   *prometheus.CounterVec
	renews   *prometheus.CounterVec
	overlapC prometheus.Counter
	stolenC  prometheus.Counter
	faults   *prometheus.CounterVec
	holders  prometheus.Gauge
}

func newStats() *stats {
	return &stats{
		start: time.Now(),
		acquires: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "lock_acquisitions_total",
			Help: "SET NX attempts, by outcome.",
		}, []string{"result"}),
		writes: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "lock_downstream_writes_total",
			Help: "Writes reaching the store the lock protects: ok, corrupt (from a worker that no longer held the lock), rejected (by a fence).",
		}, []string{"result"}),
		renews: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "lock_renewals_total",
			Help: "Lease renewals, by outcome. A lost renewal is a lock that expired under its holder.",
		}, []string{"result"}),
		overlapC: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "lock_overlaps_total",
			Help: "Entries into a critical section somebody else was already inside.",
		}),
		stolenC: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "lock_stolen_releases_total",
			Help: "Releases that deleted a lock the releaser did not hold.",
		}),
		faults: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "lock_injected_faults_total",
			Help: "Faults injected into the workers: a stop-the-world pause, or a worker dying with the lock held.",
		}, []string{"kind"}),
		holders: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "lock_holders",
			Help: "Workers inside a critical section right now, all jobs together.",
		}),
	}
}

func (s *stats) register(reg *prometheus.Registry) {
	reg.MustRegister(s.acquires, s.writes, s.renews, s.overlapC, s.stolenC, s.faults, s.holders)
}

func (s *stats) acquire(result string) { s.acquires.WithLabelValues(result).Inc() }
func (s *stats) renew(result string)   { s.renews.WithLabelValues(result).Inc() }

func (s *stats) overlap() {
	s.overlapC.Inc()
	s.count(&s.overlaps)
}

func (s *stats) stolen() {
	s.stolenC.Inc()
	s.count(&s.stolens)
}

func (s *stats) holding(d float64) { s.holders.Add(d) }

func (s *stats) stale() { s.count(&s.stales) }
func (s *stats) abort() { s.count(&s.aborts) }

func (s *stats) pause() { s.faults.WithLabelValues("pause").Inc() }
func (s *stats) crash() { s.faults.WithLabelValues("crash").Inc() }

func (s *stats) write(result string) {
	s.writes.WithLabelValues(result).Inc()
	switch result {
	case "corrupt":
		s.count(&s.corrupts)
		s.count(&s.runs)
	case "rejected":
		s.count(&s.rejects)
		s.count(&s.runs)
	default:
		s.count(&s.runs)
	}
}

func (s *stats) count(n *int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.frozen {
		*n++
	}
}

// freeze stops counting, so the metrics endpoint staying up until the process
// exits cannot add to a report already printed.
func (s *stats) freeze() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.frozen = true
}

func printHeader() {
	fmt.Println("variant\t| runs\t| overlaps\t| stolen releases\t| stale writes\t| corrupt writes\t| rejected\t| aborted\t| worst idle")
}

func (s *stats) printRow(cfg config, g *guard) {
	gap := g.gap(time.Now())
	s.mu.Lock()
	defer s.mu.Unlock()
	fmt.Printf("%s\t| %d\t| %d\t| %d\t| %d\t| %d\t| %d\t| %d\t| %s\n",
		cfg.label, s.runs, s.overlaps, s.stolens, s.stales, s.corrupts, s.rejects, s.aborts,
		gap.Round(100*time.Millisecond))
}
