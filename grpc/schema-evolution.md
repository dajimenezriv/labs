# Protobuf schema evolution

- [The setup](#the-setup)
- [1. Why protobuf fails silently](#1-why-protobuf-fails-silently)
- [7. The fix: reserved and safe evolution rules](#7-the-fix-reserved-and-safe-evolution-rules)
  - [Deploy order](#deploy-order)
- [8. buf breaking in CI](#8-buf-breaking-in-ci)
  - [Expected results](#expected-results)
- [Interview answers](#interview-answers)

## The setup

- `alerts.v1.AlertService` over grpc-go with the default codec (binary protobuf). Some clients use JSON through grpc-gateway.
- **v1 client** (the on-call dashboard, which also decides who gets paged) talks to a **v2 server**. This happens in every rolling deploy, and indefinitely with mobile apps or other teams' services.
- **1000 alerts**, ids **3,000,000,001..3,000,001,000**. The id sequence passed int32's max (2,147,483,647), which is why v2 widens `id`.
- Severity mix: **700 LOW, 200 HIGH, 100 CRITICAL**. **500** alerts are acknowledged, all by user **42**.
- Old clients create **100** alerts during the rollout, each with `created_at_ms = 1790240400000` (2026-09-24T09:00:00Z) and a `note`.

v1 and v2 of the same file. Every change here looks harmless in review:

```proto
// v1
enum Severity {
  SEVERITY_UNSPECIFIED = 0;
  SEVERITY_LOW = 1;
  SEVERITY_HIGH = 2;
}

message Alert {
  int32 id = 1;
  string sensor_id = 2;
  double value = 3;          // 21.4
  Severity severity = 4;
  int64 created_at_ms = 5;
  string note = 6;
}
```

## 1. Why protobuf fails silently

| wire type | id  | used by                                                  |
| --------- | --- | -------------------------------------------------------- |
| VARINT    | 0   | int32, int64, uint32, uint64, sint32, sint64, bool, enum |
| I64       | 1   | double, fixed64, sfixed64                                |
| LEN       | 2   | string, bytes, embedded messages, packed repeated        |
| I32       | 5   | float, fixed32, sfixed32                                 |

```proto
enum Severity {
  SEVERITY_UNSPECIFIED = 0;
  SEVERITY_LOW = 1;
  SEVERITY_HIGH = 2;
}

message Alert {
  int64 id = 1;
  string device_id = 2;
  double value = 3;
  Severity severity = 4;
}
```

```
08 80 bc c1 96 0b    field 1 VARINT  id = 3000000000
12 04 73 2d 31 37    field 2 LEN 4   "s-17"
# Add the value example
20 03                field 4 VARINT  severity = 3
```

- A reused number with different wire type or missing field is set to `nil`.
- A reused number with the same wire type is undetectable at runtime. It's the one change that corrupts data in both directions.

## 7. The fix: reserved and safe evolution rules

v2, done right:

```proto
enum Severity {
  SEVERITY_UNSPECIFIED = 0;
  SEVERITY_LOW = 1;
  SEVERITY_HIGH = 2;
  SEVERITY_CRITICAL = 3;
}

message Alert {
  reserved 6;
  reserved "note";

  int32 id = 1;                                   // widened later, readers first
  string sensor_id = 2;                           // not renamed
  double value = 3;                               // type unchanged
  Severity severity = 4;
  int64 created_at_ms = 5 [deprecated = true];    // still written until no reader uses it
  google.protobuf.Timestamp created_at = 7;
  int64 acked_by = 8;                             // new number
  string unit = 9;                                // what the string value was for
}
```

`reserved` makes the compiler refuse a reuse, of the number and of the name (the name matters for JSON and text format):

```
$ protoc -I . alerts.proto
alerts.proto: Field "owner" uses reserved number 6.
alerts.proto: Suggested field numbers for alerts.v1.Alert: 7

$ buf build
proto/alerts/v1/alerts.proto:27:18:use of reserved field number `6`
proto/alerts/v1/alerts.proto:27:10:use of reserved message field name
```

The rules:

| you want to...            | don't                                  | do                                                                                                                |
| ------------------------- | -------------------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| remove a field            | delete the line                        | delete it and `reserved N; reserved "name";`                                                                      |
| change what a field means | reuse its number                       | new number, `[deprecated = true]` on the old one, write both until every reader moved, then remove and reserve    |
| change a field's type     | edit the type                          | same as above: a new field                                                                                        |
| widen int32 → int64       | ship the writer first                  | ship every reader first; writers emit values > 2^31 − 1 only after                                                |
| rename a field            | rename it if JSON or FieldMask is used | keep the name. `json_name = "sensorId"` keeps the JSON key, but `sensor_id` input and FieldMask paths still break |
| add an enum value         | emit it right away                     | readers get a `default` that fails safe; ship readers; then emit                                                  |
| add a field               | —                                      | always safe with a new number                                                                                     |

The reader-side enum fix: unknown means "assume the worst", not "do nothing".

```go
switch a.GetSeverity() {
case alertsv1.Severity_SEVERITY_LOW:
	ticket(a)
case alertsv1.Severity_SEVERITY_HIGH:
	page(a)
default: // UNSPECIFIED, or a value added after this build
	log.Warn("unknown severity, paging", "severity", int32(a.GetSeverity()))
	page(a)
}
```

### Deploy order

**Whoever reads the change ships first.**

| change is in... | reader   | ships first |
| --------------- | -------- | ----------- |
| a request       | server   | server      |
| a response      | client   | clients     |
| a Kafka record  | consumer | consumers   |

- For responses, "clients first" may mean never: mobile apps and other teams don't upgrade on your schedule. Then the old field stays populated until the minimum supported client version stops reading it.
- `[deprecated = true]` only marks the generated getter deprecated (a lint warning in Go). It doesn't change the wire.

**`reserved` stops the one change that can't be detected at runtime. The deploy order handles the rest.**

## 8. buf breaking in CI

`reserved` only helps if someone remembers it. `buf breaking` compares the schema against the base branch and fails the build.

```yaml
# buf.yaml
version: v2
modules:
  - path: proto
breaking:
  use:
    - WIRE_JSON
```

```yaml
# .github/workflows/buf.yaml
on: pull_request
jobs:
  breaking:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0 # buf needs the base branch in .git
      - run: |
          curl -sSL https://github.com/bufbuild/buf/releases/latest/download/buf-Linux-x86_64 -o buf
          chmod +x buf
          ./buf breaking --against '.git#ref=origin/main'
```

The naive v2 against main:

```
$ buf breaking --against '.git#ref=origin/main'
proto/alerts/v1/alerts.proto:12:1:Previously present field "6" with name "note" on message "Alert" was deleted without reserving the name "note".
proto/alerts/v1/alerts.proto:12:1:Previously present field "6" with name "note" on message "Alert" was deleted without reserving the number "6".
proto/alerts/v1/alerts.proto:13:3:Field "1" with name "id" on message "Alert" changed type from "int32" to "int64".
proto/alerts/v1/alerts.proto:14:10:Field "2" on message "Alert" changed name from "sensor_id" to "device_id".
proto/alerts/v1/alerts.proto:15:3:Field "3" with name "value" on message "Alert" changed type from "double" to "string".
proto/alerts/v1/alerts.proto:17:9:Field "5" on message "Alert" changed name from "created_at_ms" to "acked_by".
$ echo $?
100
```

(Trimmed: the `json_name` lines and the doc links repeat the above.) Exit code **100** means breaking changes; the CI step fails.

Categories, from strictest to loosest:

| category    | protects                                                         | use it when                                    |
| ----------- | ---------------------------------------------------------------- | ---------------------------------------------- |
| `FILE`      | generated code, per file (buf's v2 default)                      | you publish the generated code as a library    |
| `PACKAGE`   | generated code, per package (moving types between files is fine) | same, with freedom to move types               |
| `WIRE_JSON` | binary and JSON encoding                                         | the service has JSON clients or uses FieldMask |
| `WIRE`      | binary encoding only                                             | binary only, and every consumer regenerates    |

### Expected results

What each category reports for each change in this lab:

| change                                |     `FILE`     | `WIRE_JSON` | `WIRE` |
| ------------------------------------- | :------------: | :---------: | :----: |
| reuse 5 (`int64` → `int64`, new name) |       ✗        |      ✗      | passes |
| `int32 id` → `int64`                  |       ✗        |      ✗      | passes |
| `double value` → `string`             |       ✗        |      ✗      |   ✗    |
| rename `sensor_id` → `device_id`      |       ✗        |      ✗      | passes |
| delete `note`, not reserved           |       ✗        |      ✗      |   ✗    |
| delete `note`, reserved               |       ✗        |   passes    | passes |
| add `SEVERITY_CRITICAL`               |     passes     |   passes    | passes |
| fixed v2 (section 7)                  | ✗ (the delete) |   passes    | passes |

- **`WIRE` misses the reuse.** A reused `int64` is byte-for-byte the old field. `FILE` and `WIRE_JSON` catch it only because the name changed. Someone who reuses a number and keeps the name gets through every category, which is why `reserved` is still needed.
- **`WIRE` passes `int32 → int64`**, because it's wire-compatible by the spec. The truncation in section 3 is a deploy-order problem.
- **`FILE` rejects even a reserved delete** (`FIELD_NO_DELETE`), because removing a field removes a getter from generated code.
- **No category catches a new enum value.** That's reader code, not schema.

**buf breaking catches schema changes. It can't catch deploy order or reader code, so it's a CI gate, not a proof of compatibility.**

## Interview answers

**"How do you evolve a protobuf schema safely?"**
Only add fields, with new numbers. Never reuse a number: delete the field and `reserved` both the number and the name. To change a field's type or meaning, add a new field, write both until every reader has moved, then delete the old one and reserve it. `buf breaking` runs in CI against main to enforce it.

**"What's the most dangerous change?"**
Reusing a field number with the same wire type. Readers decode it as the old field with no error, in both directions. Here, an old client's `created_at_ms` became the new server's `acked_by`, so alerts were born acknowledged and never paged.

**"Why doesn't protobuf error on a mismatch?"**
Tolerance is the design goal. A field number the reader doesn't know, or one with the wrong wire type, becomes an unknown field and the value is left at zero. proto3 doesn't send zero values, so "missing" and "default" look the same.

**"Is renaming a field safe?"**
In binary, yes: names aren't on the wire. In JSON, no: protojson uses the name, so an old client's `sensorId` is rejected with `unknown field`, or silently dropped with grpc-gateway's `DiscardUnknown: true`. FieldMask paths break too.

**"What happens when you add an enum value?"**
Old readers get a value they have no case for: `Severity(3)` in Go, `UNRECOGNIZED` in Java, the default value for closed enums, a decode error in strict JSON. It's wire-compatible and no tool flags it. The fix is in readers: a `default` branch that fails safe, shipped before any writer emits the new value.

**"Which side do you deploy first?"**
The reader of the change. Request changes: server first. Response changes: clients first, which for mobile means keeping the old field populated until the oldest supported version is gone.

**"What does buf breaking not catch?"**
New enum values, deploy order (int32 → int64 is wire-compatible), and in the `WIRE` category a reused number with the same type. Pick `WIRE_JSON` if anything speaks JSON, `FILE` if you ship generated code as a library.
