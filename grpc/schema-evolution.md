# Protobuf schema evolution

- [The setup](#the-setup)
- [1. Why protobuf fails silently](#1-why-protobuf-fails-silently)
  - [Two directions](#two-directions)
- [2. Reusing a field number](#2-reusing-a-field-number)
  - [Expected results](#expected-results)
- [3. Changing a field type](#3-changing-a-field-type)
  - [Expected results](#expected-results-1)
- [4. Renaming a field](#4-renaming-a-field)
  - [Expected results](#expected-results-2)
- [5. Removing a field](#5-removing-a-field)
  - [Expected results](#expected-results-3)
- [6. Adding an enum value](#6-adding-an-enum-value)
  - [Expected results](#expected-results-4)
- [7. The fix: reserved and safe evolution rules](#7-the-fix-reserved-and-safe-evolution-rules)
  - [Deploy order](#deploy-order)
- [8. buf breaking in CI](#8-buf-breaking-in-ci)
  - [Expected results](#expected-results-5)
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

```proto
// v2
enum Severity {
  SEVERITY_UNSPECIFIED = 0;
  SEVERITY_LOW = 1;
  SEVERITY_HIGH = 2;
  SEVERITY_CRITICAL = 3;     // new value
}

message Alert {
  int64 id = 1;              // widened: ids passed 2^31
  string device_id = 2;      // renamed
  string value = 3;          // "21.4", so it can carry text
  Severity severity = 4;
  int64 acked_by = 5;        // created_at_ms moved elsewhere, number reused
                             // note deleted
}
```

## 1. Why protobuf fails silently

**Names are not on the wire. Only field numbers and a 3-bit wire type are.** Each field is `tag, value`, where `tag = field_number << 3 | wire_type`:

| wire type | id  | used by                                                  |
| --------- | --- | -------------------------------------------------------- |
| VARINT    | 0   | int32, int64, uint32, uint64, sint32, sint64, bool, enum |
| I64       | 1   | double, fixed64, sfixed64                                |
| LEN       | 2   | string, bytes, embedded messages, packed repeated        |
| I32       | 5   | float, fixed32, sfixed32                                 |

The v2 server's alert, byte by byte:

```
08 80 bc c1 96 0b    field 1 VARINT  id = 3000000000
12 04 73 2d 31 37    field 2 LEN 4   "s-17"
1a 04 32 31 2e 34    field 3 LEN 4   "21.4"
20 03                field 4 VARINT  severity = 3
28 2a                field 5 VARINT  acked_by = 42
```

What a reader does with each field:

| the reader's schema has...      | what happens                                                     |
| ------------------------------- | ---------------------------------------------------------------- |
| no such number                  | kept as an **unknown field**, value unset (zero)                 |
| the number, same wire type      | decoded **as the reader's type**, whatever the writer meant      |
| the number, different wire type | protobuf-go: kept as an **unknown field**, value unset. No error |

- `proto.Unmarshal` returns `nil` in all three rows. Protobuf was designed so old and new code can read each other's messages; the price is that "I don't understand this" and "this is empty" look the same.
- proto3 doesn't send zero values, so "field is 0" and "field was never sent" are the same bytes.
- Unknown fields are preserved on re-encode (proto3 dropped them in 3.0–3.4, restored in 3.5). A proxy that decodes and re-encodes passes them through. Code that copies known fields into a struct or a DB row drops them.

### Two directions

|                         | reader     | writer     | in gRPC                                    |
| ----------------------- | ---------- | ---------- | ------------------------------------------ |
| **backward compatible** | new schema | old schema | new server reading an old client's request |
| **forward compatible**  | old schema | new schema | old client reading a new server's response |

A rolling deploy needs both. So does anything stored: a Kafka topic or a cache written by v1 is read by v2 for as long as it's retained.

## 2. Reusing a field number

v2 moved `created_at_ms` to a new field and gave number 5 to `acked_by`. Both are `int64`, so the wire type matches and every reader decodes without complaint:

```go
var a alertsv1.Alert // old client
proto.Unmarshal(resp, &a)
time.UnixMilli(a.CreatedAtMs).UTC() // 1970-01-01T00:00:00.042Z
```

The other direction is worse. An old client's `CreateAlert` carries `created_at_ms = 1790240400000`, and the new server stores it as `acked_by`. The alert is **born acknowledged**, by a user id that doesn't exist, and the pager skips acknowledged alerts.

### Expected results

| direction                | on the wire                     | reader sees                                     | error |
| ------------------------ | ------------------------------- | ----------------------------------------------- | ----- |
| new → old, acked alert   | `acked_by = 42`                 | `created_at_ms = 42` → 1970-01-01T00:00:00.042Z | none  |
| new → old, unacked alert | nothing (zero isn't sent)       | `created_at_ms = 0` → 1970-01-01T00:00:00Z      | none  |
| old → new, `CreateAlert` | `created_at_ms = 1790240400000` | `acked_by = 1790240400000`                      | none  |

- **1000/1000** alerts show a 1970 timestamp in the old dashboard (500 at +42ms, 500 at +0ms).
- **100/100** alerts created by old clients are stored acknowledged: **0 pages** for them.
- Nothing logs, nothing errors. It's found when someone asks why an alert from last night says 1970.

**A reused number with the same wire type is undetectable at runtime. It's the one change that corrupts data in both directions.**

## 3. Changing a field type

Two changes, two different failures.

**`double value` → `string value`.** I64 became LEN. The old reader finds field 3 with the wrong wire type and files it as unknown:

```
old client decodes: value=0  unknown=1a 04 32 31 2e 34   err=<nil>
```

The new server reading an old client's `double` does the same in reverse: `value = ""`, 8 bytes in unknown fields.

**`int32 id` → `int64 id`.** Both VARINT. The protobuf docs list int32/int64 as compatible, and they are, until a value doesn't fit. The old reader keeps the low 32 bits, as a C++ cast would:

```
3,000,000,001 − 2^32 = 3,000,000,001 − 4,294,967,296 = −1,294,967,295
```

The id is still unique (it's a bijection), so a map keyed by id works and nothing looks wrong until the id goes back to the server. The old client re-encodes it as a negative int32 (a 10-byte varint, sign-extended), and the new server reads `int64 −1294967295`. `AckAlert` returns `NOT_FOUND`.

The change is latent: with ids under 2^31 every test passes. It breaks the day the sequence crosses the boundary.

Common type changes and what an old reader sees:

| change          | wire types      | old reader sees                                                              |
| --------------- | --------------- | ---------------------------------------------------------------------------- |
| int32 → int64   | VARINT → VARINT | correct under 2^31, low 32 bits above                                        |
| int32 → uint32  | VARINT → VARINT | negative values become large positive ones                                   |
| int32 → sint32  | VARINT → VARINT | zigzag: an old `5` is decoded as `−3`                                        |
| double → float  | I64 → I32       | unknown field, `0`                                                           |
| double → string | I64 → LEN       | unknown field, `0`                                                           |
| string ↔ bytes  | LEN → LEN       | fine while bytes are valid UTF-8; otherwise the whole message fails to parse |
| bytes ↔ message | LEN → LEN       | fine if the bytes are that message's encoding                                |

### Expected results

| field   | direction                  |  affected | reader sees                    | error       |
| ------- | -------------------------- | --------: | ------------------------------ | ----------- |
| `value` | new → old                  | 1000/1000 | `0`                            | none        |
| `value` | old → new (`CreateAlert`)  |   100/100 | `""`                           | none        |
| `id`    | new → old                  | 1000/1000 | −1,294,967,295..−1,294,966,296 | none        |
| `id`    | old → new (`AckAlert(id)`) | 1000/1000 | negative id                    | `NOT_FOUND` |

- Wire type changes zero the field. Same-wire-type changes reinterpret it. Neither returns an error at decode time.
- The only loud failure is the round trip, and it's loud far from the cause.

**A type change is either a different wire type (the value disappears) or the same wire type with a different meaning (the value lies).**

## 4. Renaming a field

`sensor_id` → `device_id`. Binary: **nothing happens**. Field 2 is still a LEN string.

JSON is different, because protojson puts names on the wire. The JSON name is the lowerCamelCase of the field name (`sensorId`), and protojson also accepts the original name (`sensor_id`) on input. An old JSON client sends:

```json
{
  "id": 7,
  "sensorId": "s-17",
  "value": 21.4,
  "severity": "SEVERITY_HIGH",
  "createdAtMs": "1790240400000",
  "note": "fan noisy"
}
```

```go
protojson.Unmarshal(body, &alert)
// proto: (line 1:9): unknown field "sensorId"
```

- Default `protojson.UnmarshalOptions` has `DiscardUnknown: false`, so that's an error. That's the good outcome.
- grpc-gateway v2's default marshaler sets `DiscardUnknown: true`. The request succeeds and `device_id` is `""`.
- `FieldMask` paths (`update_mask: "sensor_id"`) are field names too. They stop matching.
- Generated code: `a.SensorId` no longer exists, so a client that regenerates fails to compile. That's loud and harmless.

### Expected results

| client                | transport | result for 100 old-client creates                   |
| --------------------- | --------- | --------------------------------------------------- |
| grpc-go               | binary    | 100/100 correct                                     |
| protojson defaults    | JSON      | 100/100 `InvalidArgument: unknown field "sensorId"` |
| grpc-gateway defaults | JSON      | 100/100 accepted, `device_id = ""`                  |

**A rename is wire-safe and JSON-breaking. Whether it's safe depends on whether anything speaks JSON or FieldMask to the service.**

## 5. Removing a field

v2 deleted `note = 6`. An old client still sends it:

```
new server decodes: unknown=… 32 09 66 61 6e 20 6e 6f 69 73 79   ("fan noisy", field 6)
```

The bytes survive in the message's unknown fields, but the handler builds a DB row from known fields:

```go
func (s *server) CreateAlert(ctx context.Context, req *alertsv1.CreateAlertRequest) (*alertsv1.Alert, error) {
	a := req.GetAlert()
	id, err := s.q.InsertAlert(ctx, db.InsertAlertParams{
		DeviceID: a.GetDeviceId(),
		Value:    a.GetValue(),
		Severity: int32(a.GetSeverity()),
	}) // unknown fields are not a column
	...
}
```

When the old client reads it back, `note` isn't sent, so it decodes as `""`.

The removal alone loses data only from clients that still send the field. The real damage comes later: number 6 now looks free, and the next person who adds a field takes it. That's section 2.

### Expected results

| step                     |      notes |
| ------------------------ | ---------: |
| old clients send         |        100 |
| stored by the new server |          0 |
| read back by old clients | 100 × `""` |

**Deleting a field is safe. Deleting it without reserving its number sets up the next reuse.**

## 6. Adding an enum value

v2 adds `SEVERITY_CRITICAL = 3`. proto3 enums are **open**: an unknown value is kept as its number. protobuf-go gives the old client `Severity(3)`, and `String()` prints `"3"`. The paging code was written when there were two severities:

```go
switch a.GetSeverity() {
case alertsv1.Severity_SEVERITY_HIGH:
	page(a)
case alertsv1.Severity_SEVERITY_LOW:
	ticket(a)
}
// Severity(3) matches neither: no page, no ticket, no log.
```

Other runtimes:

| runtime                          | unknown value 3 becomes                                      |
| -------------------------------- | ------------------------------------------------------------ |
| Go, proto3 (open enum)           | `Severity(3)`                                                |
| Java, proto3                     | `getSeverity()` = `UNRECOGNIZED`, `getSeverityValue()` = 3   |
| proto2 / editions `CLOSED` enum  | moved to unknown fields; getter returns the first value (0)  |
| protojson defaults               | `invalid value for enum field severity: "SEVERITY_CRITICAL"` |
| protojson `DiscardUnknown: true` | field left unset: `SEVERITY_UNSPECIFIED`                     |

- In JSON the server sends the enum as its name, `"SEVERITY_CRITICAL"`. An old protojson client fails to decode **the whole response**, so one CRITICAL alert breaks a `ListAlerts` page.
- This is why the zero value should be `_UNSPECIFIED`: when the value is lost, it falls back to "unknown", not to a real severity like LOW.

### Expected results

| client                     | pages | should page |                                   missed |
| -------------------------- | ----: | ----------: | ---------------------------------------: |
| v1, binary                 |   200 |         300 |                                      100 |
| v1, JSON via gateway       |   200 |         300 |                   100 (as `UNSPECIFIED`) |
| v1, JSON protojson default |     — |         300 | responses with a CRITICAL fail to decode |

- 700 LOW + 200 HIGH + 100 CRITICAL. The old client handles 900 and silently ignores the 100 most important ones.

**Adding an enum value is a wire-compatible change that is a behavior-breaking one. No schema tool can catch it: the fix is in the reader's code.**

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
