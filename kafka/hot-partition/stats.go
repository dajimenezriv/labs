package main

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type tp struct {
	topic     string
	partition int32
}

// stats is everything the report needs, behind one mutex. The consumers
// already sleep milliseconds per record; a lock held for microseconds does not
// move any number in this lab.
type stats struct {
	mu     sync.Mutex
	frozen bool

	lastProduced  map[tp]int64
	lastProcessed map[tp]int64
	whaleParts    map[tp]bool

	// Counts at the previous snapshot, for per-window rates.
	windowAt       time.Time
	windowProduced map[tp]int64
	windowDone     map[tp]int64
	doneCount      map[tp]int64

	seen       map[int64]bool
	tenantLast map[int]int64
	deviceLast map[string]int64

	dups              int
	deviceBreaks      int
	tenantBreaksWhale int
	tenantBreaksSmall int

	whaleLat []time.Duration
	smallLat map[tp][]time.Duration

	members, idle int
}

func newStats() *stats {
	return &stats{
		lastProduced:   map[tp]int64{},
		lastProcessed:  map[tp]int64{},
		whaleParts:     map[tp]bool{},
		windowAt:       time.Now(),
		windowProduced: map[tp]int64{},
		windowDone:     map[tp]int64{},
		doneCount:      map[tp]int64{},
		seen:           map[int64]bool{},
		tenantLast:     map[int]int64{},
		deviceLast:     map[string]int64{},
		smallLat:       map[tp][]time.Duration{},
	}
}

func (s *stats) produced(r *kgo.Record, isWhale bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := tp{r.Topic, r.Partition}
	s.lastProduced[k] = max(s.lastProduced[k], r.Offset+1)
	if isWhale {
		s.whaleParts[k] = true
	}
}

// processed checks one record against the order the producer assigned. An
// order break is a record whose sequence number is lower than one already
// processed for the same tenant (or device): the application saw event 7
// after event 8.
func (s *stats) processed(r *kgo.Record) {
	now := time.Now()
	f := strings.Split(string(r.Value), ",")
	id, _ := strconv.ParseInt(f[0], 10, 64)
	tenant, _ := strconv.Atoi(f[1])
	device := f[2]
	tseq, _ := strconv.ParseInt(f[3], 10, 64)
	dseq, _ := strconv.ParseInt(f[4], 10, 64)

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.frozen {
		return
	}
	k := tp{r.Topic, r.Partition}
	s.lastProcessed[k] = max(s.lastProcessed[k], r.Offset+1)
	s.doneCount[k]++

	// A redelivery after a rebalance is not an ordering bug in the key design,
	// so it is counted apart and kept out of the order checks.
	if s.seen[id] {
		s.dups++
		return
	}
	s.seen[id] = true

	if tseq < s.tenantLast[tenant] {
		if tenant == whale {
			s.tenantBreaksWhale++
		} else {
			s.tenantBreaksSmall++
		}
	}
	s.tenantLast[tenant] = max(s.tenantLast[tenant], tseq)
	if dseq < s.deviceLast[device] {
		s.deviceBreaks++
	}
	s.deviceLast[device] = max(s.deviceLast[device], dseq)

	lat := now.Sub(r.Timestamp)
	if tenant == whale {
		s.whaleLat = append(s.whaleLat, lat)
	} else {
		s.smallLat[k] = append(s.smallLat[k], lat)
	}
}

func (s *stats) resetWindow() {
	s.mu.Lock()
	s.windowAt = time.Now()
	s.mu.Unlock()
}

func (s *stats) freeze() {
	s.mu.Lock()
	s.frozen = true
	s.mu.Unlock()
}

type partitionRow struct {
	inRate, doneRate float64
	lag              int64
	whale            bool
}

func (s *stats) partitionWindow(topic string) []partitionRow {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	secs := now.Sub(s.windowAt).Seconds()
	rows := make([]partitionRow, partitions)
	for p := range partitions {
		k := tp{topic, int32(p)}
		rows[p] = partitionRow{
			inRate:   float64(s.lastProduced[k]-s.windowProduced[k]) / secs,
			doneRate: float64(s.doneCount[k]-s.windowDone[k]) / secs,
			lag:      s.lastProduced[k] - s.lastProcessed[k],
			whale:    s.whaleParts[k],
		}
		s.windowProduced[k] = s.lastProduced[k]
		s.windowDone[k] = s.doneCount[k]
	}
	s.windowAt = now
	return rows
}

func (s *stats) countMembers(ctx context.Context, adm *kadm.Client, g *group) error {
	o, err := owners(ctx, adm, g.name)
	if err != nil {
		return err
	}
	busy := map[string]bool{}
	for _, id := range o {
		busy[id] = true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.members += len(g.members)
	s.idle += len(g.members) - len(busy)
	return nil
}

func (s *stats) printRow(label string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var maxLag int64
	for k, n := range s.lastProduced {
		maxLag = max(maxLag, n-s.lastProcessed[k])
	}

	var allSmall []time.Duration
	var worst time.Duration
	for _, lats := range s.smallLat {
		allSmall = append(allSmall, lats...)
		worst = max(worst, p99(lats))
	}

	fmt.Printf("%s\t| %d (%d)\t| %d\t| %s\t| %s\t| %s\t| %d\t| %d / %d\t| %d\n",
		label, s.members, s.idle, maxLag,
		fmtDur(p99(s.whaleLat)), fmtDur(p99(allSmall)), fmtDur(worst),
		s.deviceBreaks, s.tenantBreaksWhale, s.tenantBreaksSmall, s.dups)
}

// p99 of what was processed. Records still in the backlog when the run ends
// have no latency yet and are not in here, so under a growing backlog this
// understates; the lag column is the honest number there.
func p99(d []time.Duration) time.Duration {
	if len(d) == 0 {
		return 0
	}
	d = slices.Clone(d)
	slices.Sort(d)
	return d[len(d)*99/100]
}

func fmtDur(d time.Duration) string {
	if d >= time.Second {
		return d.Round(100 * time.Millisecond).String()
	}
	return d.Round(time.Millisecond).String()
}
