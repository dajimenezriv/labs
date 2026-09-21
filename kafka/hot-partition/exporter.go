package main

// The broker's side of the lab, for Grafana: per-partition end offset, the
// group's committed offset and lag, and which member owns each partition.
// Same admin-protocol approach as the delivery-guarantees exporter, trimmed to
// what a hot partition looks like and extended with ownership, because "one
// partition is behind" is only half the picture without "and this member is
// the one holding it, while those two own nothing".

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
	endOffset  *prometheus.GaugeVec
	committed  *prometheus.GaugeVec
	lag        *prometheus.GaugeVec
	owner      *prometheus.GaugeVec
	memberPart *prometheus.GaugeVec
	scrapeErrs prometheus.Counter
	lastOK     prometheus.Gauge
}

func newMetrics(reg *prometheus.Registry) *metrics {
	grp := []string{"group", "topic", "partition"}
	m := &metrics{
		endOffset: prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "kafka_partition_end_offset", Help: "High watermark: the next offset to be written."}, []string{"topic", "partition"}),
		committed: prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "kafka_consumergroup_committed_offset", Help: "Offset the group has committed."}, grp),
		lag:       prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "kafka_consumergroup_lag", Help: "End offset minus committed offset."}, grp),
		owner:     prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "kafka_consumergroup_partition_owner", Help: "1 for the member currently assigned the partition."}, []string{"group", "topic", "partition", "member"}),
		// Zero is a real value here: a member in the group with nothing to do.
		memberPart: prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "kafka_consumergroup_member_partitions", Help: "Partitions assigned to each member."}, []string{"group", "member"}),
		scrapeErrs: prometheus.NewCounter(prometheus.CounterOpts{Name: "kafka_exporter_scrape_errors_total", Help: "Polls that failed."}),
		lastOK:     prometheus.NewGauge(prometheus.GaugeOpts{Name: "kafka_exporter_last_success_seconds", Help: "Unix time of the last fully successful poll."}),
	}
	reg.MustRegister(m.endOffset, m.committed, m.lag, m.owner, m.memberPart, m.scrapeErrs, m.lastOK)
	return m
}

func cmdExporter(args []string) error {
	fs := flag.NewFlagSet("exporter", flag.ExitOnError)
	brokers := fs.String("brokers", defaultBrokers, "bootstrap brokers")
	addr := fs.String("addr", ":9200", "listen address")
	prefix := fs.String("prefix", "hot-", "only export topics and groups with this name prefix")
	interval := fs.Duration("interval", time.Second, "how often to poll the cluster")
	fs.Parse(args)

	adm, closeFn, err := admin(strings.Split(*brokers, ","))
	if err != nil {
		return err
	}
	defer closeFn()

	reg := prometheus.NewRegistry()
	m := newMetrics(reg)

	go func() {
		for range time.Tick(*interval) {
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
	var topics []string
	for _, t := range md.Topics {
		if strings.HasPrefix(t.Topic, prefix) {
			topics = append(topics, t.Topic)
		}
	}

	m.endOffset.Reset()
	if len(topics) > 0 {
		ends, err := adm.ListEndOffsets(ctx, topics...)
		if err != nil {
			return err
		}
		ends.Each(func(o kadm.ListedOffset) {
			if o.Err == nil {
				m.endOffset.With(prometheus.Labels{"topic": o.Topic, "partition": strconv.Itoa(int(o.Partition))}).Set(float64(o.Offset))
			}
		})
	}

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

	m.committed.Reset()
	m.lag.Reset()
	m.owner.Reset()
	m.memberPart.Reset()
	if len(groups) == 0 {
		return nil
	}
	lags, err := adm.Lag(ctx, groups...)
	if err != nil {
		return err
	}
	for name, dg := range lags {
		for _, mem := range dg.Members {
			m.memberPart.With(prometheus.Labels{"group": name, "member": mem.ClientID}).Set(0)
		}
		for topic, parts := range dg.Lag {
			for p, gl := range parts {
				l := prometheus.Labels{"group": name, "topic": topic, "partition": strconv.Itoa(int(p))}
				m.committed.With(l).Set(float64(gl.Commit.At))
				m.lag.With(l).Set(float64(gl.Lag))
				if gl.Member != nil {
					m.owner.With(prometheus.Labels{"group": name, "topic": topic, "partition": l["partition"], "member": gl.Member.ClientID}).Set(1)
					m.memberPart.With(prometheus.Labels{"group": name, "member": gl.Member.ClientID}).Inc()
				}
			}
		}
	}
	return nil
}
