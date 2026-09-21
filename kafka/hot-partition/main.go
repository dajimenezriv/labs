// One producer, one tenant that sends most of the traffic, and a consumer group
// with more than enough total capacity to keep up. Every variant is the same
// load; the knob is always either how a record's key is chosen or which topic
// it goes to.
//
//	run      produce and consume for a while, then report lag, latency and ordering
//	exporter serve per-partition lag and ownership to Prometheus
//
// Everything is measured in-process: the producer knows what it sent and in
// what order, every member of the group is a separate franz-go client in the
// same process, and each processed record is checked against the order it was
// produced in. Separate clients means separate group members, which is all
// the broker can tell; one process means one clock and one place to count.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	defaultBrokers = "localhost:29092"
	tenants        = 100
	// Tenant 0. It has a lot of devices because it is a big customer, which is
	// the only reason re-keying by device can spread it at all.
	whale        = 0
	whaleDevices = 400
	smallDevices = 4
	saltBuckets  = 16

	partitions = 6
	// Records per second, all tenants together.
	rate = 1000
	// Time spent processing each record: ~450 records/s per member.
	work = 2 * time.Millisecond
	// Members on the dedicated whale topic when key=route.
	whaleMembers = 4
)

func main() {
	if len(os.Args) < 2 {
		die(errors.New("usage: hot-partition {run|exporter} [flags]"))
	}
	cmds := map[string]func([]string) error{
		"run":      cmdRun,
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

// step is "at this point in the run, the group should have this many members".
type step struct {
	at      time.Duration
	members int
}

func parseSteps(s string) ([]step, error) {
	var steps []step
	for part := range strings.SplitSeq(s, ",") {
		at, n, ok := strings.Cut(part, "=")
		if !ok {
			return nil, fmt.Errorf("bad -members step %q, want 60s=6", part)
		}
		d, err := time.ParseDuration(at)
		if err != nil {
			return nil, err
		}
		m, err := strconv.Atoi(n)
		if err != nil {
			return nil, err
		}
		steps = append(steps, step{d, m})
	}
	return steps, nil
}

type config struct {
	name       string
	key        string
	whaleShare float64
	steps      []step
	spikeFrom  time.Duration
	spikeTo    time.Duration
	spike      float64
	duration   time.Duration
	report     string
}

func cmdRun(args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	name := fs.String("name", "hot-skew", "topic name, also the group name prefix")
	key := fs.String("key", "tenant", "tenant | device | salt | route")
	whaleShare := fs.Float64("whale", 0.8, "share of traffic from the whale tenant; 0 spreads it evenly")
	members := fs.String("members", "0s=3", "group size over time: 0s=3,60s=6")
	spikeFrom := fs.Duration("spike-from", 0, "start of a whale traffic spike")
	spikeTo := fs.Duration("spike-to", 0, "end of the spike")
	spike := fs.Float64("spike", 1, "whale traffic multiplier during the spike")
	duration := fs.Duration("duration", 120*time.Second, "how long to produce")
	report := fs.String("report", "partitions", "partitions | row | header")
	label := fs.String("label", "", "row label")
	fs.Parse(args)

	if *report == "header" {
		fmt.Println("variant\t| members (idle)\t| max lag\t| whale p99\t| small p99\t| small p99, worst partition\t| device order breaks\t| tenant order breaks (whale / small)\t| dup")
		return nil
	}
	if !slices.Contains([]string{"tenant", "device", "salt", "route"}, *key) {
		return fmt.Errorf("-key must be tenant, device, salt or route, got %q", *key)
	}
	steps, err := parseSteps(*members)
	if err != nil {
		return err
	}

	cfg := config{
		name: *name, key: *key, whaleShare: *whaleShare, steps: steps,
		spikeFrom: *spikeFrom, spikeTo: *spikeTo, spike: *spike,
		duration: *duration, report: *report,
	}
	st, err := run(cfg)
	if err != nil {
		return err
	}
	if cfg.report == "row" {
		st.printRow(*label)
	}
	return nil
}

func run(cfg config) (*stats, error) {
	adm, closeAdm, err := admin([]string{defaultBrokers})
	if err != nil {
		return nil, err
	}
	defer closeAdm()

	topics := []string{cfg.name}
	if cfg.key == "route" {
		topics = append(topics, cfg.name+"-whale")
	}
	for _, t := range topics {
		if err := recreate(adm, t); err != nil {
			return nil, err
		}
	}

	st := newStats()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	var groups []*group
	mainGroup := &group{cfg: cfg, st: st, topic: cfg.name, name: cfg.name + "-g"}
	groups = append(groups, mainGroup)
	if cfg.key == "route" {
		wh := &group{cfg: cfg, st: st, topic: cfg.name + "-whale", name: cfg.name + "-whale-g"}
		groups = append(groups, wh)
		if err := wh.scaleTo(ctx, &wg, whaleMembers); err != nil {
			return nil, err
		}
	}

	// The first step is the group's starting size, and it gets to finish
	// joining before the clock starts. Otherwise the first seconds of every
	// latency number would be the initial rebalance, identical in every
	// variant and about none of them.
	if err := mainGroup.scaleTo(ctx, &wg, cfg.steps[0].members); err != nil {
		return nil, err
	}
	for _, g := range groups {
		if err := waitAssigned(ctx, adm, g); err != nil {
			return nil, err
		}
	}

	prodDone := make(chan error, 1)
	start := time.Now()
	st.resetWindow()
	go func() { prodDone <- produce(ctx, cfg, st, start) }()

	// Walk the rest of the schedule. Before every change, and once more at the
	// end, take a snapshot, so each table shows the state the previous group
	// size settled into rather than the moment of the change.
	for _, s := range cfg.steps[1:] {
		time.Sleep(time.Until(start.Add(s.at)))
		if cfg.report == "partitions" {
			if err := snapshot(ctx, adm, mainGroup, st, time.Since(start)); err != nil {
				return nil, err
			}
		}
		if err := mainGroup.scaleTo(ctx, &wg, s.members); err != nil {
			return nil, err
		}
	}

	if err := <-prodDone; err != nil {
		return nil, err
	}
	if cfg.report == "partitions" {
		if err := snapshot(ctx, adm, mainGroup, st, time.Since(start)); err != nil {
			return nil, err
		}
	}
	// Idle members are counted from the broker's own view of the assignment,
	// before anyone leaves the group and triggers a rebalance that changes it.
	for _, g := range groups {
		if err := st.countMembers(ctx, adm, g); err != nil {
			return nil, err
		}
	}

	// No draining. The backlog left at the end is the result, and waiting for
	// it to clear would only add the drain time to every latency number.
	st.freeze()
	cancel()
	for _, g := range groups {
		g.close()
	}
	wg.Wait()
	return st, nil
}

func waitAssigned(ctx context.Context, adm *kadm.Client, g *group) error {
	for range 300 {
		o, err := owners(ctx, adm, g.name)
		if err == nil && len(o) == partitions {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("group %s never got all %d partitions assigned", g.name, partitions)
}

func admin(brokers []string) (*kadm.Client, func(), error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return nil, nil, err
	}
	return kadm.NewClient(cl), cl.Close, nil
}

// recreate gives every variant a clean topic, so offsets and lag start at
// zero. The group needs nothing: deleting a topic deletes the offsets every
// group had committed for it.
func recreate(adm *kadm.Client, topic string) error {
	ctx := context.Background()
	if _, err := adm.DeleteTopics(ctx, topic); err != nil {
		return err
	}
	// Deletion is asynchronous on the controller; creating again too early
	// races it and comes back as TOPIC_ALREADY_EXISTS.
	for range 50 {
		resp, err := adm.CreateTopic(ctx, int32(partitions), 1, nil, topic)
		if err == nil && resp.Err == nil {
			return waitTopicID(ctx, adm, topic, resp.ID)
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("could not create topic %s", topic)
}

// waitTopicID holds until the broker's metadata describes the topic that was
// just created. For a moment after a delete and a create under the same name
// it can still describe the deleted one, and a consumer that picks up that id
// fetches UNKNOWN_TOPIC_ID in a loop.
func waitTopicID(ctx context.Context, adm *kadm.Client, topic string, id kadm.TopicID) error {
	for range 100 {
		md, err := adm.Metadata(ctx, topic)
		if err == nil {
			if t, ok := md.Topics[topic]; ok && t.Err == nil && t.ID == id {
				return nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("metadata for %s never showed the new topic id", topic)
}

// produce sends at a steady rate, split between the whale and everyone else,
// and assigns every record its sequence number within its tenant and within
// its device. Sequence numbers are assigned here, in one goroutine, so they
// are the order the application meant.
func produce(ctx context.Context, cfg config, st *stats, start time.Time) error {
	// Stock client: acks=all, idempotent, and the default partitioner, which
	// hashes a record's key with murmur2 exactly like the Java client does. The
	// same key lands on the same partition from either language.
	cl, err := kgo.NewClient(kgo.SeedBrokers(defaultBrokers))
	if err != nil {
		return err
	}
	defer cl.Close()

	var (
		tenantSeq = make([]int64, tenants)
		deviceSeq = map[string]int64{}
		id        int64
		whaleDue  float64
		smallDue  float64
		sendErr   error
		errOnce   sync.Once
	)

	send := func(tenant int) {
		devices := smallDevices
		if tenant == whale {
			devices = whaleDevices
		}
		device := fmt.Sprintf("t%02d/d%03d", tenant, rand.IntN(devices))
		id++
		tenantSeq[tenant]++
		deviceSeq[device]++

		topic, key := cfg.name, fmt.Sprintf("t%02d", tenant)
		switch cfg.key {
		case "device":
			if tenant == whale {
				key = device
			}
		case "salt":
			// Salting only the key you know is hot is the usual form: everyone
			// else keeps the key, and the order, they had.
			if tenant == whale {
				key = fmt.Sprintf("%s#%d", key, rand.IntN(saltBuckets))
			}
		case "route":
			if tenant == whale {
				topic, key = cfg.name+"-whale", device
			}
		}

		value := fmt.Sprintf("%d,%d,%s,%d,%d", id, tenant, device, tenantSeq[tenant], deviceSeq[device])
		rec := &kgo.Record{Topic: topic, Key: []byte(key), Value: []byte(value), Timestamp: time.Now()}
		cl.Produce(ctx, rec, func(r *kgo.Record, err error) {
			if err != nil {
				errOnce.Do(func() { sendErr = err })
				return
			}
			st.produced(r, cfg.whaleShare > 0 && tenant == whale)
		})
	}

	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	last := start
	for now := range tick.C {
		elapsed := now.Sub(start)
		if elapsed >= cfg.duration {
			break
		}
		dt := now.Sub(last).Seconds()
		last = now

		whaleRate := rate * cfg.whaleShare
		if elapsed >= cfg.spikeFrom && elapsed < cfg.spikeTo {
			whaleRate *= cfg.spike
		}
		whaleDue += whaleRate * dt
		smallDue += rate * (1 - cfg.whaleShare) * dt

		for ; whaleDue >= 1; whaleDue-- {
			send(whale)
		}
		for ; smallDue >= 1; smallDue-- {
			tenant := 1 + rand.IntN(tenants-1)
			// With no whale, tenant 0 is just another tenant.
			if cfg.whaleShare == 0 {
				tenant = rand.IntN(tenants)
			}
			send(tenant)
		}
	}
	if err := cl.Flush(ctx); err != nil {
		return err
	}
	return sendErr
}

// group is one consumer group and the members currently in it.
type group struct {
	cfg     config
	st      *stats
	topic   string
	name    string
	members []*member
}

type member struct {
	id string
	cl *kgo.Client
}

func (g *group) scaleTo(ctx context.Context, wg *sync.WaitGroup, n int) error {
	for len(g.members) < n {
		id := fmt.Sprintf("m%d", len(g.members)+1)
		// Stock group settings: cooperative-sticky, 45s session timeout,
		// autocommit every 5s. The client id is only there so the broker's view
		// of the assignment names members the same way this report does.
		cl, err := kgo.NewClient(
			kgo.SeedBrokers(defaultBrokers),
			kgo.ConsumeTopics(g.topic),
			kgo.ConsumerGroup(g.name),
			kgo.ClientID(id),
		)
		if err != nil {
			return err
		}
		m := &member{id: id, cl: cl}
		g.members = append(g.members, m)
		wg.Go(func() { g.consume(ctx, m) })
	}
	return nil
}

// consume is the ordinary poll loop: fetch, process each record in turn, let
// autocommit follow behind. One member works through everything it was
// assigned sequentially, whichever partition it came from.
func (g *group) consume(ctx context.Context, m *member) {
	for {
		fetches := m.cl.PollFetches(ctx)
		if ctx.Err() != nil || fetches.IsClientClosed() {
			return
		}
		fetches.EachError(func(t string, p int32, err error) {
			fmt.Fprintf(os.Stderr, "%s %s: %s[%d]: %s\n", g.name, m.id, t, p, err)
		})
		for _, r := range fetches.Records() {
			if ctx.Err() != nil {
				return
			}
			// The work: a database write, an API call, anything that takes a
			// couple of milliseconds and has to happen once per record.
			time.Sleep(work)
			g.st.processed(r)
		}
	}
}

func (g *group) close() {
	for _, m := range g.members {
		m.cl.Close()
	}
}

// snapshot prints what the broker says each member owns next to what this
// process has measured for each partition since the previous snapshot.
func snapshot(ctx context.Context, adm *kadm.Client, g *group, st *stats, at time.Duration) error {
	owners, err := owners(ctx, adm, g.name)
	if err != nil {
		return err
	}
	rows := st.partitionWindow(g.topic)

	fmt.Printf("\n-- t=%s, %d members\n", at.Round(time.Second), len(g.members))
	tw := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "partition\t| owner\t| in/s\t| done/s\t| lag\t| whale")
	for p, row := range rows {
		owner := owners[int32(p)]
		if owner == "" {
			owner = "-"
		}
		w := ""
		if row.whale {
			w = "yes"
		}
		fmt.Fprintf(tw, "%d\t| %s\t| %.0f\t| %.0f\t| %d\t| %s\n", p, owner, row.inRate, row.doneRate, row.lag, w)
	}
	tw.Flush()
	var idle []string
	busy := map[string]bool{}
	for _, o := range owners {
		busy[o] = true
	}
	for _, m := range g.members {
		if !busy[m.id] {
			idle = append(idle, m.id)
		}
	}
	if len(idle) > 0 {
		fmt.Printf("idle members: %s\n", strings.Join(idle, " "))
	}
	return nil
}

func owners(ctx context.Context, adm *kadm.Client, groupName string) (map[int32]string, error) {
	lags, err := adm.Lag(ctx, groupName)
	if err != nil {
		return nil, err
	}
	out := map[int32]string{}
	for _, dg := range lags {
		for _, parts := range dg.Lag {
			for p, gl := range parts {
				if gl.Member != nil {
					out[p] = gl.Member.ClientID
				}
			}
		}
	}
	return out, nil
}
