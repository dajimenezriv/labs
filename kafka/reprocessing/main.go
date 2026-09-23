// A numbered, keyed sequence through a consumer that fails on purpose, with
// every successful handling written to a sink file and the file scored at the
// end.
//
//	topics   recreate readings, its retry tiers and its DLQ
//	produce  send ids 1..N at a fixed rate, spread over K keys
//	consume  run the blocking consumer or the tiered pipeline against a
//	         handler with injected failures, until it goes quiet
//	verify   score a sink file
//	dlq      list | merge | purge the dead letter queue
//	offsets  print the live topic's log start and end offsets
//
// A record's value is "id,seq,produced_ms": seq is its position within its
// key, produced_ms is when it was first sent. Both ride along unchanged through
// every retry topic, so the sink can tell how late and how out of order a
// record arrived however many hops it took.
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

var brokers = []string{"localhost:29092"}

const partitions = 3

func main() {
	if len(os.Args) < 2 {
		die(errors.New("usage: reprocessing {topics|produce|consume|verify|dlq|offsets} [flags]"))
	}
	cmds := map[string]func([]string) error{
		"topics":  cmdTopics,
		"produce": cmdProduce,
		"consume": cmdConsume,
		"verify":  cmdVerify,
		"dlq":     cmdDLQ,
		"offsets": cmdOffsets,
	}
	run, ok := cmds[os.Args[1]]
	if !ok {
		die(fmt.Errorf("unknown command %q", os.Args[1]))
	}
	if err := run(os.Args[2:]); err != nil {
		die(err)
	}
}

func die(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}

func admin() (*kadm.Client, func(), error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return nil, nil, err
	}
	return kadm.NewClient(cl), cl.Close, nil
}

// topics recreates every lab topic, so a count is a count and not a delta
// against whatever the last run left behind.
func cmdTopics(args []string) error {
	fs := flag.NewFlagSet("topics", flag.ExitOnError)
	retention := fs.String("retention-ms", "", "retention.ms for the live topic; empty keeps the broker default (7 days)")
	fs.Parse(args)

	adm, closeFn, err := admin()
	if err != nil {
		return err
	}
	defer closeFn()

	ctx := context.Background()
	if _, err := adm.DeleteTopics(ctx, labTopics()...); err != nil {
		return err
	}
	for _, topic := range labTopics() {
		configs := map[string]*string{}
		if topic == liveTopic && *retention != "" {
			configs["retention.ms"] = retention
		}
		// Deletion is asynchronous on the controller; creating again too
		// early races it and comes back as TOPIC_ALREADY_EXISTS.
		created := false
		for range 50 {
			resp, err := adm.CreateTopic(ctx, partitions, 1, configs, topic)
			if err == nil && resp.Err == nil {
				created = true
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
		if !created {
			return fmt.Errorf("could not create topic %s", topic)
		}
	}
	fmt.Fprintf(os.Stderr, "topics: %s (%d partitions)\n", strings.Join(labTopics(), ", "), partitions)
	return nil
}

func cmdProduce(args []string) error {
	fs := flag.NewFlagSet("produce", flag.ExitOnError)
	n := fs.Int("n", 6000, "how many ids to send")
	rate := fs.Int("rate", 200, "records per second")
	keys := fs.Int("keys", 30, "distinct keys")
	fs.Parse(args)

	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.DefaultProduceTopic(liveTopic))
	if err != nil {
		return err
	}
	defer cl.Close()

	var (
		wg     sync.WaitGroup
		failed atomic.Int64
	)
	ctx := context.Background()
	interval := time.Second / time.Duration(*rate)
	start := time.Now()
	for id := 1; id <= *n; id++ {
		// A steady rate rather than a burst: latency only means something if
		// records arrive while the consumer is working, the way traffic does.
		time.Sleep(time.Until(start.Add(time.Duration(id-1) * interval)))
		m := msg{id: id, key: keyOf(id, *keys), seq: seqOf(id, *keys), producedMs: time.Now().UnixMilli()}
		wg.Add(1)
		cl.Produce(ctx, &kgo.Record{Key: []byte(m.key), Value: m.encode()}, func(_ *kgo.Record, err error) {
			defer wg.Done()
			if err != nil {
				failed.Add(1)
			}
		})
	}
	wg.Wait()
	if failed.Load() > 0 {
		return fmt.Errorf("%d records failed to produce", failed.Load())
	}
	fmt.Fprintf(os.Stderr, "produce: %d records in %s\n", *n, time.Since(start).Round(time.Millisecond))
	return nil
}

func keyOf(id, keys int) string { return fmt.Sprintf("k%02d", (id-1)%keys) }
func seqOf(id, keys int) int    { return (id-1)/keys + 1 }

type msg struct {
	id, seq    int
	key        string
	producedMs int64
}

func (m msg) encode() []byte { return fmt.Appendf(nil, "%d,%d,%d", m.id, m.seq, m.producedMs) }

func decode(rec *kgo.Record) msg {
	var m msg
	fmt.Sscanf(string(rec.Value), "%d,%d,%d", &m.id, &m.seq, &m.producedMs)
	m.key = string(rec.Key)
	return m
}

// faults is the downstream the handler calls, and every way it can go wrong.
// Failures are chosen by id, not at random, so the same run fails the same
// records every time and variants are comparable.
type faults struct {
	// Every flakyEvery-th id fails its first flakyTimes attempts, then works:
	// a timeout, a 503, a deadlock victim. Retryable.
	flakyEvery, flakyTimes int
	// This id fails on every attempt, retryably: a downstream that 500s on
	// one specific payload. Waiting will never fix it, and nothing says so.
	poison int
	// Every bugEvery-th id fails as non-retryable: the consumer cannot parse a
	// field that a newer producer started sending. Fixed by the next deploy.
	bugEvery int
	// Ids in this range succeed but write a wrong result: a bug that errors
	// nowhere and is noticed hours later.
	badFrom, badTo int
	// What one call costs when it works.
	work time.Duration
}

func (f faults) call(m msg, attempt int) (flag string, err error) {
	time.Sleep(f.work)
	switch {
	case m.id == f.poison:
		return "", errors.New("downstream: 500 internal server error")
	case f.bugEvery > 0 && m.id%f.bugEvery == 0:
		return "", fmt.Errorf("%w: unknown field \"unit\"", errNonRetryable)
	case f.flakyEvery > 0 && m.id%f.flakyEvery == 0 && attempt <= f.flakyTimes:
		return "", errors.New("downstream: 503 service unavailable")
	case m.id >= f.badFrom && m.id <= f.badTo:
		return "bad", nil
	}
	return "ok", nil
}

// sink is the handler's side effect: one line per successful handling, in
// the order they happened. It is append-only on purpose; verify reads it both
// as an append-only table and as an upsert table keyed by id.
type sink struct {
	mu sync.Mutex
	f  *os.File
}

func (s *sink) write(m msg, attempt int, flag string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fmt.Fprintf(s.f, "%d,%s,%d,%d,%d,%d,%s\n", m.id, m.key, m.seq, m.producedMs, time.Now().UnixMilli(), attempt, flag)
}

func cmdConsume(args []string) error {
	fs := flag.NewFlagSet("consume", flag.ExitOnError)
	mode := fs.String("mode", "tiered", "blocking | tiered")
	group := fs.String("group", "lab", "consumer group (tiers use <group>.retry.N)")
	sinkPath := fs.String("sink", "out/sink", "file to append handled records to")
	idle := fs.Duration("idle", 10*time.Second, "finish after this long with no handling attempts")
	deadline := fs.Duration("deadline", 0, "finish after this long regardless; 0 never")
	var f faults
	fs.IntVar(&f.flakyEvery, "flaky-every", 0, "every Nth id fails its first -flaky-times attempts")
	fs.IntVar(&f.flakyTimes, "flaky-times", 2, "")
	fs.IntVar(&f.poison, "poison", 0, "this id always fails, retryably")
	fs.IntVar(&f.bugEvery, "bug-every", 0, "every Nth id fails non-retryably")
	fs.IntVar(&f.badFrom, "bad-from", 0, "first id written with a wrong result")
	fs.IntVar(&f.badTo, "bad-to", -1, "last id written with a wrong result")
	fs.DurationVar(&f.work, "work", time.Millisecond, "cost of one successful call")
	fs.Parse(args)

	if err := os.MkdirAll("out", 0o755); err != nil {
		return err
	}
	file, err := os.OpenFile(*sinkPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()
	out := &sink{f: file}

	// Any attempt counts as activity, failed ones included: a blocking
	// consumer stuck on a poison pill is busy, not idle, and only the deadline
	// ends it.
	var lastAttempt atomic.Int64
	var handled, deadLettered atomic.Int64
	handle := func(rec *kgo.Record, attempt int) error {
		lastAttempt.Store(time.Now().UnixNano())
		m := decode(rec)
		flag, err := f.call(m, attempt)
		if err != nil {
			return err
		}
		out.write(m, attempt, flag)
		handled.Add(1)
		return nil
	}

	var consumers []*consumer
	switch *mode {
	case "blocking":
		c, err := newConsumer(brokers, *group, liveTopic)
		if err != nil {
			return err
		}
		defer c.client.Close()
		consumers = []*consumer{c}
	case "tiered":
		p, err := newPipeline(brokers, *group, func(rec *kgo.Record) {
			m := decode(rec)
			out.write(m, retryCount(rec)+1, "dlq")
			deadLettered.Add(1)
		})
		if err != nil {
			return err
		}
		defer p.close()
		consumers = p.consumers
	default:
		return fmt.Errorf("-mode must be blocking or tiered, got %q", *mode)
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for _, c := range consumers {
		wg.Go(func() { c.run(ctx, handle) })
	}

	start := time.Now()
	for range time.Tick(200 * time.Millisecond) {
		last := lastAttempt.Load()
		if last > 0 && time.Since(time.Unix(0, last)) > *idle {
			break
		}
		if *deadline > 0 && time.Since(start) > *deadline {
			fmt.Fprintf(os.Stderr, "consume: deadline %s reached\n", *deadline)
			break
		}
	}
	cancel()
	wg.Wait()
	fmt.Fprintf(os.Stderr, "consume: %s handled=%d dead-lettered=%d\n", *mode, handled.Load(), deadLettered.Load())
	return nil
}

type line struct {
	msg
	handledMs int64
	attempt   int
	flag      string
}

func readSink(path string) ([]line, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var lines []line
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		p := strings.Split(sc.Text(), ",")
		if len(p) != 7 {
			continue
		}
		var l line
		l.id, _ = strconv.Atoi(p[0])
		l.key = p[1]
		l.seq, _ = strconv.Atoi(p[2])
		l.producedMs, _ = strconv.ParseInt(p[3], 10, 64)
		l.handledMs, _ = strconv.ParseInt(p[4], 10, 64)
		l.attempt, _ = strconv.Atoi(p[5])
		l.flag = p[6]
		lines = append(lines, l)
	}
	return lines, sc.Err()
}

func cmdVerify(args []string) error {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	view := fs.String("view", "pipeline", "pipeline | dlq | replay")
	label := fs.String("label", "", "row label")
	path := fs.String("sink", "out/sink", "sink file")
	n := fs.Int("n", 6000, "ids produced")
	keys := fs.Int("keys", 30, "distinct keys produced")
	header := fs.Bool("header", false, "print the column header and exit")
	fs.Parse(args)

	switch *view {
	case "pipeline":
		if *header {
			fmt.Println("variant\t| delivered\t| dlq\t| dup\t| drain\t| p50 first try\t| p99 first try\t| p50 retried\t| out of order\t| stale keys")
			return nil
		}
	case "dlq":
		// Latency means nothing here: a merge resets the retry count, so a
		// merged record looks like a first try that took minutes.
		if *header {
			fmt.Println("moment\t| delivered\t| dlq\t| dup\t| out of order\t| stale keys")
			return nil
		}
	case "replay":
		if *header {
			fmt.Println("moment\t| sink\t| rows\t| distinct ids\t| duplicate rows\t| wrong rows")
			return nil
		}
	default:
		return fmt.Errorf("-view must be pipeline, dlq or replay, got %q", *view)
	}

	lines, err := readSink(*path)
	if err != nil {
		return err
	}
	var ok []line
	dlq := 0
	for _, l := range lines {
		if l.flag == "dlq" {
			dlq++
		} else {
			ok = append(ok, l)
		}
	}

	delivered := map[int]line{}
	for _, l := range ok {
		// Last write wins: this is what an upsert keyed by id would hold.
		delivered[l.id] = l
	}
	dup := len(ok) - len(delivered)

	if *view == "replay" {
		wrongAppend, wrongUpsert := 0, 0
		for _, l := range ok {
			if l.flag == "bad" {
				wrongAppend++
			}
		}
		for _, l := range delivered {
			if l.flag == "bad" {
				wrongUpsert++
			}
		}
		fmt.Printf("%s\t| append (INSERT)\t| %d\t| %d\t| %d\t| %d\n", *label, len(ok), len(delivered), dup, wrongAppend)
		fmt.Printf("%s\t| upsert by id\t| %d\t| %d\t| %d\t| %d\n", *label, len(delivered), len(delivered), 0, wrongUpsert)
		return nil
	}

	var firstTry, retried []int64
	var minProduced, maxHandled int64
	for i, l := range ok {
		if i == 0 || l.producedMs < minProduced {
			minProduced = l.producedMs
		}
		maxHandled = max(maxHandled, l.handledMs)
		if l.attempt == 1 {
			firstTry = append(firstTry, l.handledMs-l.producedMs)
		} else {
			retried = append(retried, l.handledMs-l.producedMs)
		}
	}

	// Out of order: handled after a later record of the same key already had
	// been. Stale: the key's last write is not its latest event, so a
	// last-write-wins table ends up holding an old value.
	highest := map[string]int{}
	outOfOrder := 0
	final := map[string]int{}
	for _, l := range ok {
		if l.seq < highest[l.key] {
			outOfOrder++
		}
		highest[l.key] = max(highest[l.key], l.seq)
		final[l.key] = l.seq
	}
	latest := map[string]int{}
	for id := 1; id <= *n; id++ {
		latest[keyOf(id, *keys)] = seqOf(id, *keys)
	}
	stale := 0
	for key, seq := range latest {
		if final[key] != seq {
			stale++
		}
	}

	if *view == "dlq" {
		fmt.Printf("%s\t| %d/%d\t| %d\t| %d\t| %d\t| %d\n", *label, len(delivered), *n, dlq, dup, outOfOrder, stale)
		return nil
	}
	fmt.Printf("%s\t| %d/%d\t| %d\t| %d\t| %s\t| %s\t| %s\t| %s\t| %d\t| %d\n",
		*label, len(delivered), *n, dlq, dup,
		ms(maxHandled-minProduced), pct(firstTry, 50), pct(firstTry, 99), pct(retried, 50),
		outOfOrder, stale)
	return nil
}

func pct(xs []int64, p int) string {
	if len(xs) == 0 {
		return "-"
	}
	s := slices.Sorted(slices.Values(xs))
	return ms(s[(len(s)-1)*p/100])
}

func ms(v int64) string {
	if v < 1000 {
		return fmt.Sprintf("%dms", v)
	}
	return fmt.Sprintf("%.1fs", float64(v)/1000)
}

// offsets prints the live topic's first and next offset summed over its
// partitions: how much of the log still exists, and how much was ever written.
func cmdOffsets(args []string) error {
	adm, closeFn, err := admin()
	if err != nil {
		return err
	}
	defer closeFn()

	ctx := context.Background()
	starts, err := adm.ListStartOffsets(ctx, liveTopic)
	if err == nil {
		err = starts.Error()
	}
	if err != nil {
		return err
	}
	ends, err := adm.ListEndOffsets(ctx, liveTopic)
	if err == nil {
		err = ends.Error()
	}
	if err != nil {
		return err
	}
	var start, end int64
	starts.Each(func(o kadm.ListedOffset) { start += o.Offset })
	ends.Each(func(o kadm.ListedOffset) { end += o.Offset })
	fmt.Printf("%d\t%d\n", start, end)
	return nil
}
