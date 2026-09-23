package main

// The answer to "who looks at the DLQ". Nothing consumes that topic, so
// without a tool it is where records go to be forgotten. Three operations,
// which is what an operator holding a paged alert actually does:
//
//	dlq list    what is in there, and why, without consuming it
//	dlq merge   republish it all onto the first retry tier
//	dlq purge   delete it all
//
// Merging into retry.1 rather than the live topic is Uber's choice: records
// that already failed do not compete with live traffic, and if they fail again
// they walk the ladder and come back here rather than looping.
//
// Merge does not consume what it merges -- no group, no commit. A second merge
// sends everything a second time. The sequence is merge, check, purge.

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func cmdDLQ(args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: dlq list|merge|purge")
	}
	ctx := context.Background()
	switch args[0] {
	case "list":
		return dlqList(ctx)
	case "merge":
		return dlqMerge(ctx)
	case "purge":
		return dlqPurge(ctx)
	}
	return fmt.Errorf("usage: dlq list|merge|purge")
}

func dlqList(ctx context.Context) error {
	records, err := drain(ctx, dlqTopic)
	if err != nil {
		return err
	}
	for _, r := range records {
		headers := slices.Clone(r.Headers)
		// Written in map order, so left alone the fields land in a different
		// order on every record.
		slices.SortFunc(headers, func(a, b kgo.RecordHeader) int { return strings.Compare(a.Key, b.Key) })
		fields := []string{fmt.Sprintf("%d/%d key=%s value=%s", r.Partition, r.Offset, r.Key, r.Value)}
		for _, h := range headers {
			fields = append(fields, fmt.Sprintf("%s=%s", h.Key, h.Value))
		}
		fmt.Println(strings.Join(fields, " "))
	}
	fmt.Printf("%d record(s) in %s\n", len(records), dlqTopic)
	return nil
}

func dlqMerge(ctx context.Context) error {
	records, err := drain(ctx, dlqTopic)
	if err != nil {
		return err
	}
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return err
	}
	defer client.Close()

	destination := retryTopic(1)
	for _, r := range records {
		// Back to the front of the ladder: the retry count is dropped so a
		// merged record gets every tier again rather than one attempt at the
		// tier it died on. The origin headers stay.
		headers := make([]kgo.RecordHeader, 0, len(r.Headers))
		for _, h := range r.Headers {
			if h.Key != retryCountHeader && h.Key != errorHeader {
				headers = append(headers, h)
			}
		}
		if err := client.ProduceSync(ctx, &kgo.Record{
			Topic: destination, Key: r.Key, Value: r.Value, Headers: headers,
		}).FirstErr(); err != nil {
			return fmt.Errorf("republish offset %d: %w", r.Offset, err)
		}
	}
	fmt.Printf("merged %d record(s) into %s; they are still in %s\n", len(records), destination, dlqTopic)
	return nil
}

// dlqPurge deletes everything before the current end of each partition.
// Records arriving during the call are past that line and survive: purge
// clears what you looked at, not what came in while you were deciding.
func dlqPurge(ctx context.Context) error {
	adm, closeFn, err := admin()
	if err != nil {
		return err
	}
	defer closeFn()

	ends, err := adm.ListEndOffsets(ctx, dlqTopic)
	if err == nil {
		err = ends.Error()
	}
	if err != nil {
		return err
	}
	responses, err := adm.DeleteRecords(ctx, ends.Offsets())
	if err != nil {
		return err
	}
	var failed error
	responses.Each(func(r kadm.DeleteRecordsResponse) {
		if r.Err != nil && failed == nil {
			failed = fmt.Errorf("partition %d: %w", r.Partition, r.Err)
		}
	})
	if failed != nil {
		return failed
	}
	fmt.Printf("purged %s\n", dlqTopic)
	return nil
}

// drain reads everything currently in a topic without a consumer group, so
// reading it twice shows the same records both times.
//
// The end offsets are the finish line. A poll on a topic read to the end just
// blocks waiting for the next record, so without knowing where the end is
// there is no telling "drained" from "quiet". The start offsets matter too:
// after a purge the log starts later than zero.
func drain(ctx context.Context, topic string) ([]*kgo.Record, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return nil, err
	}
	defer client.Close()
	adm := kadm.NewClient(client)

	starts, err := adm.ListStartOffsets(ctx, topic)
	if err == nil {
		err = starts.Error()
	}
	if err != nil {
		return nil, err
	}
	ends, err := adm.ListEndOffsets(ctx, topic)
	if err == nil {
		err = ends.Error()
	}
	if err != nil {
		return nil, err
	}

	pending := 0
	ends.Each(func(end kadm.ListedOffset) {
		if start, ok := starts.Lookup(end.Topic, end.Partition); ok {
			pending += int(end.Offset - start.Offset)
		}
	})

	records := make([]*kgo.Record, 0, pending)
	for len(records) < pending {
		fetches := client.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("read %s: %w", topic, err)
		}
		if err := fetches.Err(); err != nil {
			return nil, fmt.Errorf("read %s: %w", topic, err)
		}
		fetches.EachRecord(func(r *kgo.Record) { records = append(records, r) })
	}
	return records, nil
}
