# gRPC

- [What is gRPC (Remote Procedure Call)?](#what-is-grpc-remote-procedure-call)
- [What is protobuf?](#what-is-protobuf)
- [How to update protobuf schema?](#how-to-update-protobuf-schema)
- [How are errors handled?](#how-are-errors-handled)
- [Which side do you deploy first?](#which-side-do-you-deploy-first)

## What is gRPC (Remote Procedure Call)?

- A communication framework (how data is sent).
- Like REST or SOAP over HTTP.
- Executes functions across different servers as if they were local.
- Built on top of HTTP/2.
- Uses .proto files to define API endpoints (services).

```proto
service Alerts {
  rpc GetAlert(GetAlertRequest) returns (GetAlertResponse);
}
```

## What is protobuf?

- A data serialization format (how data is packed).
- Like JSON or XML.
- Structured code objects into tiny, fast binary packages.
- Uses .proto files to define data structures.

```proto
message GetAlertRequest { int64 id = 1; }
```

```
# field = 1
# type = VARINT
# value = 3000000000
08 80 bc c1 96 0b
```

## How to update protobuf schema?

- Only add fields, with new numbers.
- Never reuse a number: delete the field and `reserved` both the number and the name.
- `buf breaking` runs in CI against main to enforce it.

```proto
message Data {
  reserved 1;
  reserved "id";
  // A lint warning in Go.
  string device_id = 2 [deprecated = true];
}
```

## How are errors handled?

- Tolerance is the design goal. There are no errors.
- A field the reader doesn't know, or a reused number with a different wire type, reads as the zero value.
- Zero value: `0`, `""`, the enum's first value.

## Which side do you deploy first?

The reader of the change. Request changes: server first. Response changes: clients first, which for mobile means keeping the old field populated until the oldest supported version is gone.
