// A numbered sequence, produced once and consumed once, with the two sets
// compared at the end. Every variant in this lab is the same rig with one knob
// moved; the knob is always either where the producer's ack comes from or
// where the consumer's commit sits relative to its work.
//
//	topic    (re)create the lab topic with a given replication factor / min ISR
//	leader   print the node id currently leading partition 0
//	produce  send 1..N, record the ids the broker ACKNOWLEDGED
//	consume  read the topic, append each id to a sink file, optionally die
//	verify   compare the two files
//	exporter serve the cluster's leader / ISR / group-lag numbers to Prometheus
//
// The producer records acked ids, not attempted ids, and that distinction is
// the lab. A record that returns an error is a handled failure: the caller
// knows, and can retry. A record that returns success and never arrives is
// data loss, and nothing upstream will ever find out.
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

const defaultBrokers = "localhost:29092,localhost:29093,localhost:29094"

func main() {
	if len(os.Args) < 2 {
		die(errors.New("usage: delivery-guarantees {topic|leader|produce|consume|verify} [flags]"))
	}

	cmds := map[string]func([]string) error{
		"topic":    cmdTopic,
		"leader":   cmdLeader,
		"produce":  cmdProduce,
		"consume":  cmdConsume,
		"verify":   cmdVerify,
		"exporter": cmdExporter,
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

// admin dials any broker; admin requests are routed to the right one for us.
func admin(brokers string) (*kadm.Client, func(), error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(brokers, ",")...))
	if err != nil {
		return nil, nil, err
	}
	return kadm.NewClient(cl), cl.Close, nil
}

// topic recreates the topic from scratch. Each variant gets a clean log, so a
// count is a count and not a delta against whatever the last run left behind.
func cmdTopic(args []string) error {
	fs := flag.NewFlagSet("topic", flag.ExitOnError)
	brokers := fs.String("brokers", defaultBrokers, "bootstrap brokers")
	topic := fs.String("topic", "seq", "topic name")
	partitions := fs.Int("partitions", 1, "partition count")
	rf := fs.Int("rf", 3, "replication factor")
	minISR := fs.String("min-isr", "1", "min.insync.replicas")
	fs.Parse(args)

	adm, closeFn, err := admin(*brokers)
	if err != nil {
		return err
	}
	defer closeFn()

	ctx := context.Background()
	if _, err := adm.DeleteTopics(ctx, *topic); err != nil {
		return err
	}
	// Deletion is asynchronous on the controller; creating again too early
	// races it and comes back as TOPIC_ALREADY_EXISTS.
	for range 30 {
		resp, err := adm.CreateTopic(ctx, int32(*partitions), int16(*rf), map[string]*string{
			"min.insync.replicas": minISR,
		}, *topic)
		if err == nil && resp.Err == nil {
			fmt.Printf("topic %s: partitions=%d rf=%d min.insync.replicas=%s\n", *topic, *partitions, *rf, *minISR)
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("could not create topic %s", *topic)
}

// leader answers the only question the kill script needs: which process do we
// have to kill for the kill to mean anything.
func cmdLeader(args []string) error {
	fs := flag.NewFlagSet("leader", flag.ExitOnError)
	brokers := fs.String("brokers", defaultBrokers, "bootstrap brokers")
	topic := fs.String("topic", "seq", "topic name")
	fs.Parse(args)

	adm, closeFn, err := admin(*brokers)
	if err != nil {
		return err
	}
	defer closeFn()

	md, err := adm.Metadata(context.Background(), *topic)
	if err != nil {
		return err
	}
	for _, t := range md.Topics {
		for _, p := range t.Partitions {
			if p.Partition == 0 {
				fmt.Println(p.Leader)
				return nil
			}
		}
	}
	return fmt.Errorf("no partition 0 for topic %s", *topic)
}

func cmdProduce(args []string) error {
	fs := flag.NewFlagSet("produce", flag.ExitOnError)
	brokers := fs.String("brokers", defaultBrokers, "bootstrap brokers")
	topic := fs.String("topic", "seq", "topic name")
	n := fs.Int("n", 200000, "how many ids to send")
	acks := fs.String("acks", "all", "all | one")
	out := fs.String("out", "out/produced.ids", "file to write acknowledged ids to")
	// Stock franz-go retries a retryable error until the record succeeds. That
	// is right for production and useless for measuring refusal, because
	// NOT_ENOUGH_REPLICAS is retryable and a producer aimed at a cluster that
	// will never satisfy min.insync.replicas simply blocks forever. Bounding
	// the tries rather than the time is what makes the broker's own error the
	// one that comes back, instead of a client-side timeout wrapping it.
	retries := fs.Int("retries", 0, "give up on a record after this many tries; 0 never")
	row := fs.String("row", "", "print a tab-separated result row with this label")
	fs.Parse(args)

	opts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(*brokers, ",")...),
		kgo.DefaultProduceTopic(*topic),
		// No batching delay. The window this lab measures is "acknowledged but
		// not yet replicated", and lingering would just make the producer hold
		// records it has not sent at all, which is a different thing.
		kgo.ProducerLinger(0),
	}
	if *retries > 0 {
		opts = append(opts, kgo.RecordRetries(*retries))
	}
	switch *acks {
	case "all":
		// Stock franz-go: acks=all plus the idempotent producer. Nothing to set.
	case "one":
		// Both, or neither -- franz-go rejects acks=1 on its own with
		// "idempotency requires acks=all", because a producer that retries
		// without sequence numbers is a producer that duplicates.
		opts = append(opts,
			kgo.RequiredAcks(kgo.LeaderAck()),
			kgo.DisableIdempotentWrite(),
			kgo.MaxProduceRequestsInflightPerBroker(5),
		)
	default:
		return fmt.Errorf("-acks must be all or one, got %q", *acks)
	}

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return err
	}
	defer cl.Close()

	f, err := create(*out)
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriter(f)

	var (
		mu      sync.Mutex
		failed  atomic.Int64
		firstEr atomic.Value
		wg      sync.WaitGroup
	)

	ctx := context.Background()
	start := time.Now()
	for i := 1; i <= *n; i++ {
		wg.Add(1)
		rec := &kgo.Record{Value: []byte(strconv.Itoa(i))}
		id := i
		cl.Produce(ctx, rec, func(_ *kgo.Record, err error) {
			defer wg.Done()
			if err != nil {
				failed.Add(1)
				firstEr.CompareAndSwap(nil, err.Error())
				return
			}
			// Acknowledged. From here on the application believes this id is
			// durable, and whether that belief survives is the experiment.
			mu.Lock()
			fmt.Fprintln(w, id)
			mu.Unlock()
		})
	}
	wg.Wait()
	mu.Lock()
	err = w.Flush()
	mu.Unlock()
	if err != nil {
		return err
	}

	acked := int64(*n) - failed.Load()
	fmt.Fprintf(os.Stderr, "produced: sent=%d acked=%d failed=%d in %s\n",
		*n, acked, failed.Load(), time.Since(start).Round(time.Millisecond))
	if e := firstEr.Load(); e != nil {
		fmt.Fprintf(os.Stderr, "produced: first error: %s\n", e)
	}
	if *row != "" {
		reason := "-"
		if e := firstEr.Load(); e != nil {
			reason = e.(string)
		}
		fmt.Printf("%s\t| %d\t| %d\t| %d\t| %s\n", *row, *n, acked, failed.Load(), reason)
	}
	return nil
}

func cmdConsume(args []string) error {
	fs := flag.NewFlagSet("consume", flag.ExitOnError)
	brokers := fs.String("brokers", defaultBrokers, "bootstrap brokers")
	topic := fs.String("topic", "seq", "topic name")
	group := fs.String("group", "lab", "consumer group")
	sink := fs.String("sink", "out/consumed.ids", "file to append processed ids to")
	commit := fs.String("commit", "default", "greedy | default | manual")
	crashAfter := fs.Int("crash-after", 0, "exit(9) immediately after processing this many records; 0 never")
	perRecord := fs.Duration("per-record", 0, "simulated work per record")
	idle := fs.Duration("idle", 4*time.Second, "finish after this long with no records")
	fs.Parse(args)

	opts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(*brokers, ",")...),
		kgo.ConsumeTopics(*topic),
		kgo.ConsumerGroup(*group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		// Stock is 45s. A killed member holds up the rebalance until its
		// session expires, and every crash variant here restarts one, so the
		// default turns a 2s experiment into a 45s wait. 6s is the broker's
		// own group.min.session.timeout.ms floor.
		kgo.SessionTimeout(6 * time.Second),
	}
	switch *commit {
	case "greedy":
		// Commit whatever has been polled, on a timer, whether or not it has
		// been processed. This is what enable.auto.commit=true means in the
		// Java client and in librdkafka; franz-go makes you ask for it.
		opts = append(opts, kgo.GreedyAutoCommit(), kgo.AutoCommitInterval(100*time.Millisecond))
	case "default":
		// franz-go's own default: autocommit every 5s, but only offsets from a
		// PREVIOUS poll, so records in hand are never committed under you.
		opts = append(opts, kgo.AutoCommitInterval(100*time.Millisecond))
	case "manual":
		opts = append(opts, kgo.DisableAutoCommit())
	default:
		return fmt.Errorf("-commit must be greedy, default or manual, got %q", *commit)
	}

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return err
	}
	defer cl.Close()

	// O_APPEND: the second run of a crash variant continues the same sink, so
	// the file ends up holding exactly what the pipeline as a whole delivered,
	// restarts included.
	f, err := appendTo(*sink)
	if err != nil {
		return err
	}
	defer f.Close()

	ctx := context.Background()
	processed := 0
	lastRecord := time.Now()
	for {
		pollCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		fetches := cl.PollRecords(pollCtx, 500)
		cancel()
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if !errors.Is(e.Err, context.DeadlineExceeded) {
					fmt.Fprintf(os.Stderr, "consume: %s\n", e.Err)
				}
			}
		}

		recs := fetches.Records()
		if len(recs) == 0 {
			if time.Since(lastRecord) > *idle {
				break
			}
			continue
		}
		lastRecord = time.Now()

		for _, r := range recs {
			if *perRecord > 0 {
				time.Sleep(*perRecord)
			}
			// The write IS the processing: once the line is in the file, the
			// work is done and the effect is visible downstream.
			if _, err := f.Write(append(r.Value, '\n')); err != nil {
				return err
			}
			processed++
			if *crashAfter > 0 && processed == *crashAfter {
				fmt.Fprintf(os.Stderr, "consume: processed %d, dying without a clean exit\n", processed)
				// No Close, no leave-group, no final commit. Whatever the
				// commit placement has already done for us is all we get.
				os.Exit(9)
			}
		}

		if *commit == "manual" {
			// After the work, which is the whole point: the offset advances
			// only over records whose effect is already durable.
			if err := cl.CommitRecords(ctx, recs...); err != nil {
				return err
			}
		}
	}

	if *commit != "manual" {
		// franz-go's autocommit only ever commits a previous poll, so a clean
		// shutdown has to flush the last one by hand.
		if err := cl.CommitUncommittedOffsets(ctx); err != nil {
			return err
		}
	}
	fmt.Fprintf(os.Stderr, "consume: processed %d, exited cleanly\n", processed)
	return nil
}

// verify is the entire point of writing ids down: lost is an id the producer
// was told was durable and the consumer never saw, duplicated is an id the
// consumer processed more than once. One line per variant.
func cmdVerify(args []string) error {
	fs := flag.NewFlagSet("verify", flag.ExitOnError)
	label := fs.String("label", "", "row label")
	producedFile := fs.String("produced", "out/produced.ids", "acknowledged ids")
	consumedFile := fs.String("consumed", "out/consumed.ids", "processed ids")
	header := fs.Bool("header", false, "print the column header and exit")
	fs.Parse(args)

	cols := "variant\t| acked\t| delivered\t| lost\t| dup\t| phantom\t| first gap"
	if *header {
		fmt.Println(cols)
		return nil
	}

	acked, err := readIDs(*producedFile)
	if err != nil {
		return err
	}
	consumed, err := readIDs(*consumedFile)
	if err != nil {
		return err
	}

	ackedSet := map[int]bool{}
	for _, id := range acked {
		ackedSet[id] = true
	}
	seen := map[int]int{}
	for _, id := range consumed {
		seen[id]++
	}

	var lost []int
	for id := range ackedSet {
		if seen[id] == 0 {
			lost = append(lost, id)
		}
	}
	slices.Sort(lost)

	dup, phantom := 0, 0
	for id, count := range seen {
		if count > 1 {
			dup += count - 1
		}
		// Acknowledged as failed (or never sent) and delivered anyway: the
		// other direction of the same lie.
		if !ackedSet[id] {
			phantom++
		}
	}

	gap := "-"
	if len(lost) > 0 {
		gap = strconv.Itoa(lost[0])
		if len(lost) > 1 {
			gap = fmt.Sprintf("%d..%d", lost[0], lost[len(lost)-1])
		}
	}
	fmt.Printf("%s\t| %d\t| %d\t| %d\t| %d\t| %d\t| %s\n",
		*label, len(ackedSet), len(consumed), len(lost), dup, phantom, gap)
	return nil
}

func create(path string) (*os.File, error) {
	if err := os.MkdirAll(dir(path), 0o755); err != nil {
		return nil, err
	}
	return os.Create(path)
}

func appendTo(path string) (*os.File, error) {
	if err := os.MkdirAll(dir(path), 0o755); err != nil {
		return nil, err
	}
	return os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
}

func dir(path string) string {
	if i := strings.LastIndex(path, "/"); i > 0 {
		return path[:i]
	}
	return "."
}

func readIDs(path string) ([]int, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var ids []int
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		id, err := strconv.Atoi(line)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		ids = append(ids, id)
	}
	return ids, sc.Err()
}
