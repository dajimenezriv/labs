# Labs

Each lab reproduces one production situation, measures it, and explains it in a markdown writeup. The labs serve two purposes:

- **Learning**: understand how the thing actually behaves, not how the docs say it behaves.
- **Interviews**: turn "have you dealt with X?" into a concrete story with a number attached.

## Layout

Each lab is self-contained. Labs do not share code or containers; copy what you need instead of importing it from another lab.

## Keep it minimal

This is the main rule. A lab contains only what the situation needs.

- Nothing that isn't part of the situation or needed to measure it: no extra services, config layers, packages, abstractions, logging, or error handling for things the lab never triggers.
- Before adding a file, dependency, flag, or container, check that the writeup uses it. If it doesn't, leave it out.
- One flat `package main` is the default. Split files by concern only when a file gets hard to read.
- Prometheus/Grafana only when a graph over time shows something a results table can't.

## Mimic production

- Use stock defaults. Change a setting only when that setting is what the lab is about, and add a comment saying why.
- Never configure something to fail just to show the failure. If the defaults already prevent the problem, that is the finding and it goes in the writeup.
- Scaling things down to make runs fast (lower `max_connections`, shorter TTLs, fewer CPUs) is fine. State it in the setup section.

## Measure, don't claim

- Every claim in a writeup is backed by output from a real run. Never invent or estimate numbers. If you couldn't run it, say so and leave the table empty.
- Scripts print results in a shape that pastes straight into a markdown table.
- Compare variants on the same rig, changing one thing at a time: baseline, break it, fix it.

## Writeup style

- `# Title`, a table of contents for longer writeups, then the command that starts everything (`docker compose up`).
- `## The setup`: bullets with the numbers that matter (sizes, rates, limits) and why they make the situation reproducible.
- Numbered sections, one per scenario. Each has the command to run, the results table, then short bullets explaining what the numbers mean. Bold the takeaway.
- Explain the mechanism (why it happens), not just the result. Include the error codes, default values, and config names an interviewer would probe.
- Terse bullets over prose. Include code only for the few lines the point depends on.

## Code conventions

- Go: check `go.mod` for the version and use the newest stdlib affordances (`wg.Go`, etc.).
- Go errors: skip `if err != nil` for errors the lab isn't about (setup, marshalling, closing, etc.) and discard them with `_`. Only check the errors the writeup explains or the measurement counts. The labs are not about learning Go, so the code should show only what matters to the situation. Exception: the `golang` lab, where error handling is part of the point.
- Bash scripts start with `set -euo pipefail` and `cd "$(dirname "$0")"`. Shared helpers for a lab go in its `lib.sh`.
- Build binaries into a temp dir or gitignore them, along with any `out/` directory.
