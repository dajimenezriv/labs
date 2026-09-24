# Protobuf schema evolution

- [1. Protobuf fails silently](#1-protobuf-fails-silently)
- [2. The fix: reserved and safe evolution rules](#2-the-fix-reserved-and-safe-evolution-rules)
- [3. buf breaking in CI](#3-buf-breaking-in-ci)
  - [Expected results](#expected-results)
- [Interview answers](#interview-answers)

## 1. Protobuf fails silently

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

## 2. The fix: reserved and safe evolution rules

```proto
enum Severity {
  SEVERITY_UNSPECIFIED = 0;
  SEVERITY_LOW = 1;
  SEVERITY_HIGH = 2;
  SEVERITY_CRITICAL = 3;
}

message Alert {
  reserved 1;
  reserved "id";

  // First [deprecated = true], then remove and reserve.
  // A lint warning in Go.
  string uuid = 2 [deprecated = true];
}
```

## 3. buf breaking in CI

`reserved` only helps if someone remembers it. `buf breaking` compares the schema against the base branch and fails the build.

```yaml
# buf.yaml
version: v2
modules:
  - path: proto
breaking:
  use:
    - PACKAGE # the one we used at Kurita
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

```
$ buf breaking --against '.git#ref=origin/main'

proto/alerts/v1/alerts.proto:12:1:Previously present field "6" with name "note" on message "Alert" was deleted without reserving the name "note".
proto/alerts/v1/alerts.proto:12:1:Previously present field "6" with name "note" on message "Alert" was deleted without reserving the number "6".
proto/alerts/v1/alerts.proto:13:3:Field "1" with name "id" on message "Alert" changed type from "int32" to "int64".
proto/alerts/v1/alerts.proto:14:10:Field "2" on message "Alert" changed name from "sensor_id" to "device_id".
proto/alerts/v1/alerts.proto:15:3:Field "3" with name "value" on message "Alert" changed type from "double" to "string".
proto/alerts/v1/alerts.proto:17:9:Field "5" on message "Alert" changed name from "created_at_ms" to "acked_by".
```

Categories, from strictest to loosest:

| category    | protects                                    | use it when                                 |
| ----------- | ------------------------------------------- | ------------------------------------------- |
| `FILE`      | generated code, per file (buf's v2 default) | you publish the generated code as a library |
| `PACKAGE`   | generated code, per package                 | same, with freedom to move types            |
| `WIRE_JSON` | binary and JSON encoding                    | the service has JSON clients                |
| `WIRE`      | binary encoding only                        | binary only, and every consumer regenerates |

### Expected results

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

## Interview answers

**"How do you evolve a protobuf schema safely?"**
Only add fields, with new numbers. Never reuse a number: delete the field and `reserved` both the number and the name. To change a field's type or meaning, add a new field, write both until every reader has moved, then delete the old one and reserve it. `buf breaking` runs in CI against main to enforce it.

**"What's the most dangerous change?"**
Reusing a field number with the same wire type. Readers decode it as the old field with no error.

**"Why doesn't protobuf error on a mismatch?"**
Tolerance is the design goal. A field number the reader doesn't know, or one with the wrong wire type, becomes an unknown field and the value is left at zero. proto3 doesn't send zero values, so "missing" and "default" look the same.

**"Is renaming a field safe?"**

- In binary, yes: names aren't on the wire.
- In JSON, no: protojson uses the name.

**"Which side do you deploy first?"**
The reader of the change.

- Request changes: server first.
- Response changes: clients first.
