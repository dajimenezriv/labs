package main

// A Prometheus exporter for the handful of numbers this lab is about: who
// leads each partition, how many replicas are in sync, where each consumer
// group has committed, and how far behind that leaves it.
//
// Written against the admin protocol rather than the brokers' JMX beans. The
// usual answer is the Prometheus JMX exporter running as a javaagent inside
// every broker, and it sees strictly more -- request latencies, ISR shrink
// RATES, messages-in per second, unclean election counts. What it does not see
// is consumer group lag, which is the metric this lab needs most, and it costs
// a jar inside every broker image. Everything below comes from Metadata,
// ListEndOffsets and OffsetFetch: the same three calls kafka-consumer-groups
// makes.

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/twmb/franz-go/pkg/kadm"
)

type metrics struct {
	leader     *prometheus.GaugeVec
	replicas   *prometheus.GaugeVec
	isr        *prometheus.GaugeVec
	underRepl  *prometheus.GaugeVec
	endOffset  *prometheus.GaugeVec
	committed  *prometheus.GaugeVec
	lag        *prometheus.GaugeVec
	groupState *prometheus.GaugeVec
	members    *prometheus.GaugeVec
	brokers    prometheus.Gauge
	scrapeErrs prometheus.Counter
	lastOK     prometheus.Gauge
}

func newMetrics(reg *prometheus.Registry) *metrics {
	part := []string{"topic", "partition"}
	grp := []string{"group", "topic", "partition"}
	m := &metrics{
		leader:    prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "kafka_partition_leader", Help: "Broker id currently leading the partition."}, part),
		replicas:  prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "kafka_partition_replicas", Help: "Configured replica count."}, part),
		isr:       prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "kafka_partition_in_sync_replicas", Help: "Replicas currently in the ISR."}, part),
		underRepl: prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "kafka_partition_under_replicated", Help: "1 when in-sync replicas < configured replicas."}, part),
		endOffset: prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "kafka_partition_end_offset", Help: "High watermark: the next offset to be written."}, part),
		committed: prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "kafka_consumergroup_committed_offset", Help: "Offset the group has committed."}, grp),
		lag:       prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "kafka_consumergroup_lag", Help: "End offset minus committed offset."}, grp),
		// State as a label rather than a number, so a dashboard can show
		// PreparingRebalance by name instead of by magic constant.
		groupState: prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "kafka_consumergroup_state", Help: "1 for the group's current state."}, []string{"group", "state"}),
		members:    prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "kafka_consumergroup_members", Help: "Members currently in the group."}, []string{"group"}),
		brokers:    prometheus.NewGauge(prometheus.GaugeOpts{Name: "kafka_brokers", Help: "Brokers currently registered in the cluster."}),
		scrapeErrs: prometheus.NewCounter(prometheus.CounterOpts{Name: "kafka_exporter_scrape_errors_total", Help: "Polls that failed."}),
		// Without this, a dead exporter and a healthy cluster look identical:
		// the gauges simply hold their last value forever.
		lastOK: prometheus.NewGauge(prometheus.GaugeOpts{Name: "kafka_exporter_last_success_seconds", Help: "Unix time of the last fully successful poll."}),
	}
	reg.MustRegister(m.leader, m.replicas, m.isr, m.underRepl, m.endOffset,
		m.committed, m.lag, m.groupState, m.members, m.brokers, m.scrapeErrs, m.lastOK)
	return m
}

func cmdExporter(args []string) error {
	fs := flag.NewFlagSet("exporter", flag.ExitOnError)
	brokers := fs.String("brokers", defaultBrokers, "bootstrap brokers")
	addr := fs.String("addr", ":9200", "listen address")
	prefix := fs.String("prefix", "seq-", "only export topics and groups with this name prefix")
	// Every event in this lab is seconds long. A poll slower than the failure
	// cannot describe it.
	interval := fs.Duration("interval", time.Second, "how often to poll the cluster")
	fs.Parse(args)

	adm, closeFn, err := admin(*brokers)
	if err != nil {
		return err
	}
	defer closeFn()

	reg := prometheus.NewRegistry()
	m := newMetrics(reg)

	go func() {
		for range time.Tick(*interval) {
			// Bounded, because a poll that hangs on a dead broker until the
			// next one starts would silently halve the resolution.
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			if err := poll(ctx, adm, m, *prefix); err != nil {
				m.scrapeErrs.Inc()
				fmt.Fprintf(os.Stderr, "exporter: %s\n", err)
			} else {
				m.lastOK.Set(float64(time.Now().Unix()))
			}
			cancel()
		}
	}()

	fmt.Fprintf(os.Stderr, "exporter: listening on %s, polling every %s\n", *addr, *interval)
	http.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	return http.ListenAndServe(*addr, nil)
}

func poll(ctx context.Context, adm *kadm.Client, m *metrics, prefix string) error {
	md, err := adm.Metadata(ctx)
	if err != nil {
		return err
	}
	m.brokers.Set(float64(len(md.Brokers)))

	// Reset before repopulating so a deleted topic or a finished group stops
	// reporting instead of freezing at its last value. Only on success --
	// a failed poll holds the previous numbers, and lastOK is how you tell.
	m.leader.Reset()
	m.replicas.Reset()
	m.isr.Reset()
	m.underRepl.Reset()
	m.endOffset.Reset()

	var topics []string
	for _, t := range md.Topics {
		if !strings.HasPrefix(t.Topic, prefix) {
			continue
		}
		topics = append(topics, t.Topic)
		for _, p := range t.Partitions {
			l := prometheus.Labels{"topic": t.Topic, "partition": strconv.Itoa(int(p.Partition))}
			m.leader.With(l).Set(float64(p.Leader))
			m.replicas.With(l).Set(float64(len(p.Replicas)))
			m.isr.With(l).Set(float64(len(p.ISR)))
			m.underRepl.With(l).Set(b2f(len(p.ISR) < len(p.Replicas)))
		}
	}
	if len(topics) == 0 {
		return nil
	}

	ends, err := adm.ListEndOffsets(ctx, topics...)
	if err != nil {
		return err
	}
	ends.Each(func(o kadm.ListedOffset) {
		if o.Err != nil {
			return
		}
		m.endOffset.With(prometheus.Labels{"topic": o.Topic, "partition": strconv.Itoa(int(o.Partition))}).Set(float64(o.Offset))
	})

	listed, err := adm.ListGroups(ctx)
	if err != nil {
		return err
	}
	var groups []string
	for _, g := range listed.Groups() {
		if strings.HasPrefix(g, prefix) {
			groups = append(groups, g)
		}
	}
	if len(groups) == 0 {
		return nil
	}

	lags, err := adm.Lag(ctx, groups...)
	if err != nil {
		return err
	}
	m.committed.Reset()
	m.lag.Reset()
	m.groupState.Reset()
	m.members.Reset()
	for name, dg := range lags {
		m.groupState.With(prometheus.Labels{"group": name, "state": dg.State}).Set(1)
		m.members.With(prometheus.Labels{"group": name}).Set(float64(len(dg.Members)))
		for topic, parts := range dg.Lag {
			for p, gl := range parts {
				l := prometheus.Labels{"group": name, "topic": topic, "partition": strconv.Itoa(int(p))}
				m.committed.With(l).Set(float64(gl.Commit.At))
				m.lag.With(l).Set(float64(gl.Lag))
			}
		}
	}
	return nil
}

func b2f(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
