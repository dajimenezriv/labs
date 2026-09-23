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

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func cmdDLQ(op string) {
	switch op {
	case "list":
		dlqList()
	case "merge":
		dlqMerge()
	case "purge":
		dlqPurge()
	}
}

func dlqList() {
	records := drain(dlqTopic)
	for _, r := range records {
		fields := []string{fmt.Sprintf("%d/%d key=%s value=%s", r.Partition, r.Offset, r.Key, r.Value)}
		for _, h := range r.Headers {
			fields = append(fields, fmt.Sprintf("%s=%s", h.Key, h.Value))
		}
		// Header order is map order; sort so every record reads the same way.
		slices.Sort(fields[1:])
		fmt.Println(strings.Join(fields, " "))
	}
	fmt.Printf("%d record(s) in %s\n", len(records), dlqTopic)
}

func dlqMerge() {
	records := drain(dlqTopic)
	producer := client()
	for _, r := range records {
		// Back to the front of the ladder: the retry count is dropped so a
		// merged record gets every tier again rather than one attempt at the
		// tier it died on. The origin headers stay.
		headers := slices.DeleteFunc(r.Headers, func(h kgo.RecordHeader) bool {
			return h.Key == retryCountHeader || h.Key == errorHeader
		})
		producer.ProduceSync(context.Background(), &kgo.Record{
			Topic: retryTopic(1), Key: r.Key, Value: r.Value, Headers: headers,
		})
	}
	fmt.Printf("merged %d record(s) into %s; they are still in %s\n", len(records), retryTopic(1), dlqTopic)
}

// dlqPurge deletes everything before the current end of each partition, which
// moves the log start offset. Records arriving during the call are past that
// line and survive: purge clears what you looked at, not what came in while
// you were deciding.
func dlqPurge() {
	ctx := context.Background()
	adm := admin()
	ends, _ := adm.ListEndOffsets(ctx, dlqTopic)
	adm.DeleteRecords(ctx, ends.Offsets())
	fmt.Printf("purged %s\n", dlqTopic)
}

// drain reads everything currently in a topic without a consumer group, so
// reading it twice shows the same records both times.
//
// The end offsets are the finish line. A poll on a topic read to the end just
// blocks waiting for the next record, so without knowing where the end is
// there is no telling "drained" from "quiet". The start offsets matter too:
// after a purge the log starts later than zero.
func drain(topic string) []*kgo.Record {
	ctx := context.Background()
	adm := admin()
	starts, _ := adm.ListStartOffsets(ctx, topic)
	ends, _ := adm.ListEndOffsets(ctx, topic)
	pending := 0
	ends.Each(func(end kadm.ListedOffset) {
		start, _ := starts.Lookup(end.Topic, end.Partition)
		pending += int(end.Offset - start.Offset)
	})

	cl := client(kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	defer cl.Close()
	var records []*kgo.Record
	for len(records) < pending {
		records = append(records, cl.PollFetches(ctx).Records()...)
	}
	return records
}
