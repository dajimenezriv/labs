# Golang

- [`[]byte` vs `string`](#byte-vs-string)
- [`Encoder`/`Decoder` vs `Marshal`/`Unmarshal`](#encoderdecoder-vs-marshalunmarshal)

## `[]byte` vs `string`

`string` is immutable.

- `[]byte` for data you're building or transforming. Appending in a loop to `string` is O(n²), every `+=` allocates a new string and copies. With `[]byte` or `strings.Builder` (which is a `[]byte` underneath) it's O(n).
- `string` for text you're passing around, comparing, or using as a map key.

Languages that unify the two either give up immutability or pay for copy-on-write. DOUBT.

## `Encoder`/`Decoder` vs `Marshal`/`Unmarshal`

```go
b, err := io.ReadAll(r) // io.Reader -> []byte
r := bytes.NewReader(b) // []byte -> io.Reader
r := strings.NewReader(s) // string -> io.Reader
```

Use `Decoder` when we have a reader and we only need the parsed value. Use `Unmarshal` when we have the bytes or when we need to keep the bytes. Both store everything into memory.

Examples when we need the bytes:

```go
body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
// verify HMAC signature over body, log it, store it raw...
err := json.Unmarshal(body, &v);
```

`Decoder` reads from the reader into an internal buffer that grows. Peak allocation is the full raw JSON + the decoded Go value (exactly like `ReadAll` + `Unmarshal`).

- `Unmarshal` is the value-level API (`[]byte` in).
- `Decode` is the stream-level API (`io.Reader` in).
- `Encoder.Encode` appends a newline, `Marshal` doesn't.

Benefits of `Decoder`:

- **Multiple values from one stream**. DOUBT.
- **Early abort on malformed input**. As soon as the value cannot be a JSON it stops reading. Real protection against oversized bodies is `http.MaxBytesReader`.
- **Decoder only options**. DOUBT.
- **Buffer reuse**. DOUBT.

Benefits of `Encoder`:

- Is the better default for HTTP. It writes straight to the ResponseWriter without holding the full serialized output. The encoder uses an internal buffer it reuses and flushes to the writer, so you're not necessarily materializing the whole payload at once for large or repeated writes. DOUBT. Then, do we hold something or not?

```go
w.Header().Set("Content-Type", "application/json")
json.NewEncoder(w).Encode(resp)
```
