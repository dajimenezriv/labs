How a replayable queue works??? And dlq?

1. The delivery-guarantees lab: count lost and duplicated messages end to end

Produce a numbered sequence, have the consumer write each ID to a sink, then compare sets. Run four variants: acks=1 with a leader kill mid-flight (gaps), acks=all with min.insync.replicas=2 (no gaps), auto-commit with a crash after poll (loss), and commit-after-process with a crash before commit (duplicates). Same rig, four different failure signatures.

This is the single most asked Kafka interview area. After this you can answer "how do you guarantee no message loss" with a concrete chain (producer acks, ISR, replication factor, commit placement, idempotent sink) instead of reciting config names, and you'll have a real number to quote for how many messages you lost.

2. The hot partition

Send 80% of traffic under one key. Watch lag climb on exactly one partition while the others sit at zero, then add consumer instances and watch it not help at all. Then work through the options: change the key, add a salt and lose per-key ordering, or route the whale key to dedicated capacity.

This covers partition key design, why consumers can't exceed partition count, and the ordering tradeoff, which is the "design a Kafka pipeline" interview question in disguise. It's also the most common real capacity incident, because traffic skew usually shows up only after a big customer onboards.

Do them in that order. Each one gives you a war story with a metric attached, which is what makes the answer sound like experience rather than reading.

3. Create the Uber queue system.
4. Replayable queue.