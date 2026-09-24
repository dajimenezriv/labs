# Labs

Each lab is one markdown file that explains one production situation: how it happens, what it costs, and how to fix it. There is no runnable code. The labs serve two purposes:

- **Learning**: understand how the thing actually behaves, not how the docs say it behaves.
- **Interviews**: turn "have you dealt with X?" into a concrete story with a number attached.

Model: [kafka/uber-and-replayable-dlq.md](kafka/uber-and-replayable-dlq.md).

## Layout

- One markdown file per lab, in the lab's folder. No `compose.yaml`, Go, scripts, or other files to run.
- Older labs still have code (`delivery-guarantees`, `reprocessing`, the redis labs). Leave it alone, but don't add code to new labs or extend the old code.

## Self-contained

The markdown is the only thing read, so it has to stand on its own.

- Every code snippet is complete enough to understand without the source. If a snippet calls a helper (`drain`, `admin()`, `routeHeaders`), either show it or explain what it does in one line.
- Include a code snippet wherever it links the text to the mechanism: the routing decision, the commit order, the SQL that makes a write idempotent, the CLI command an operator types. Skip code that only wires things together.
- CLI commands are written the way they'd be typed in the lab's environment (`docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh ...`).

## Numbers are derived, not measured

- Tables hold the numbers that **should** happen, derived from the setup section. Put a note under the title saying they're expected, not measured.
- Show the arithmetic behind any number that isn't obvious (`12 × 3s = 36s of stall`), so it can be repeated in an interview.
- Use `~` for anything that depends on timing or queueing, and `≥` or a range where the exact value depends on luck. Don't fake precision.
- Keep the setup small and round (6000 records, 200/s, 30 keys) so the derivations stay simple.

## Mimic production

- Describe behavior on stock defaults. Name the defaults (`max.poll.interval.ms` = 300000, `retention.ms` = 7 days).
- Never build a scenario on a setting changed just to make it fail. If the defaults prevent the problem, that is the finding.
- Scaling things down for the example (seconds instead of minutes) is fine. Say so in the setup section.

## Writeup style

- `# Title`, the expected-numbers note, a table of contents.
- `## The setup`: bullets with the numbers that matter (sizes, rates, limits, failure rates).
- Numbered sections, one per concept. Each explains the mechanism, shows the snippets it depends on, then an `### Expected results` table with short bullets on what the numbers mean. Bold the takeaway.
- Explain why it happens, not just what happens. Include the error codes, default values, and config names an interviewer would probe.
- Show both sides where there's a tradeoff: the naive approach, why it breaks, and the fix.
- End with `## Interview answers`: the likely questions, each answered in two or three sentences.
- Terse bullets over prose. Comparison tables where two things are easy to confuse (fetch position vs committed offset).
