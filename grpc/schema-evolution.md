# Protobuf schema evolution

- [1. Protobuf fails silently](#1-protobuf-fails-silently)
- [2. The fix: reserved and safe evolution rules](#2-the-fix-reserved-and-safe-evolution-rules)
- [3. buf breaking in CI](#3-buf-breaking-in-ci)
  - [Expected results](#expected-results)

## 1. Protobuf fails silently

| wire type | used by                                                  |
| --------- | -------------------------------------------------------- |
| VARINT    | int32, int64, uint32, uint64, sint32, sint64, bool, enum |
| I64       | double, fixed64, sfixed64                                |
| LEN       | string, bytes, embedded messages, packed repeated        |
| I32       | float, fixed32, sfixed32                                 |

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
08 80 bc c1 96 0b             field 1 VARINT  id = 3000000000
12 04 73 2d 31 37             field 2 LEN 4   "s-17"
19 66 66 66 66 66 66 35 40    field 3 I64     value = 21.4
20 02                         field 4 VARINT  severity = 2
```

- A field the reader doesn't know, or a reused number with a different wire type, is kept as an unknown field and reads as the zero value (`0`, `""`, the enum's first value). No error.
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
  string device_id = 2 [deprecated = true];
  double value = 3;
  Severity severity = 4;
  string uuid = 5;
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

Categories, from strictest to loosest:

| category    | protects                                    | use it when                                 |
| ----------- | ------------------------------------------- | ------------------------------------------- |
| `FILE`      | generated code, per file (buf's v2 default) | you publish the generated code as a library |
| `PACKAGE`   | generated code, per package                 | same, with freedom to move types            |
| `WIRE_JSON` | binary and JSON encoding                    | the service has JSON clients                |
| `WIRE`      | binary encoding only                        | binary only, and every consumer regenerates |

### Expected results

| change                                             | `FILE` | `PACKAGE` | `WIRE_JSON` | `WIRE` |
| -------------------------------------------------- | :----: | :-------: | :---------: | :----: |
| reuse number (`int64` → `int64`, new name)         |   ✗    |     ✗     |      ✗      | passes |
| `int32 id` → `int64`                               |   ✗    |     ✗     |      ✗      | passes |
| `double value` → `string`                          |   ✗    |     ✗     |      ✗      |   ✗    |
| delete `note`, not reserved                        |   ✗    |     ✗     |      ✗      |   ✗    |
| delete `note`, reserved                            |   ✗    |     ✗     |   passes    | passes |
| add `SEVERITY_CRITICAL`                            | passes |  passes   |   passes    | passes |
| move `Severity` to `severity.proto` (same package) |   ✗    |  passes   |   passes    | passes |
