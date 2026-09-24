# Object Storage

- [TODO](#todo)
- [Destination](#destination)
  - [Direct to the bucket](#direct-to-the-bucket)
  - [Through the service](#through-the-service)
- [Framing](#framing)
  - [Raw body + `Content-Length`](#raw-body--content-length)
  - [`multipart/form-data`](#multipartform-data)
  - [`Transfer-Encoding: chunked`](#transfer-encoding-chunked)
- [Segmentation](#segmentation)
  - [S3 multipart upload](#s3-multipart-upload)
  - [Resumable protocols](#resumable-protocols)
- [Integrity](#integrity)
- [MinIO](#minio)
- [Examples](#examples)
  - [Store](#store)
  - [Through the service + Raw body + `Content-Length`](#through-the-service--raw-body--content-length)
  - [Through the service + `multipart/form-data`](#through-the-service--multipartform-data)
  - [Direct to bucket + S3 multipart upload](#direct-to-bucket--s3-multipart-upload)

## TODO

- What should we measure in Grafana?
- `readyz` endpoint: `Ping` to the bucket. Not built — `Store` has no `Ping` and no
  service registers an object storage check. [Store](#store).

## Destination

Who receives the bytes?

### Direct to the bucket

- Scales without limit.
- Sends one or more presigned PUT urls with a lifetime.
- A single presigned `PutObject` can force the `Content-Type` in the signed headers. Multipart cannot.
- A browser needs a CORS rule on the bucket, and that rule has to expose `ETag`. Without it the client cannot read the header.

[Direct to bucket + S3 multipart upload](#direct-to-bucket--s3-multipart-upload)

### Through the service

- We can validate, transcode and more.
- It occupies one of our request slots (a 4G phone sending 5MiB holds a goroutine for 30s).

[Through the service + Raw body + `Content-Length`](#through-the-service--raw-body--content-length)

## Framing

How is the body shaped on the wire?

### Raw body + `Content-Length`

- Whole body in just a request.
- Max allowed size is 5 GiB since it's the cap for a `PutObject`.

[Through the service + Raw body + `Content-Length`](#through-the-service--raw-body--content-length)

### `multipart/form-data`

- What a browser `<form>` sends.

[Through the service + `multipart/form-data`](#through-the-service--multipartform-data)

### `Transfer-Encoding: chunked`

"I don't know how long this is". It's not faster or more streamy, it's what you send when the length is unknown up front, like piping a live transcode. We can make our endpoint reject an upload if it doesn't contain a `Content-Length` upfront. S3 does accept chunked bodies of its own, via `Content-Encoding: aws-chunked` with `x-amz-decoded-content-length` — what it never accepts is an unknown total, so the byte count has to be known either way.

## Segmentation

### S3 multipart upload

Split the object into parts, upload each independently, then send a manifest to assemble them.

- Every part but the last must be >= 5 MiB and there can be at most 10,000 of them.
- Parallel part uploads.
- Retry of a single failed part.
- Objects up to 5 TiB (a single `PutObject` caps at 5 GiB).
- Object never appears if client does not complete the upload.

[Direct to bucket + S3 multipart upload](#direct-to-bucket--s3-multipart-upload)

### Resumable protocols

Add a session ID and a "how many bytes did you get?" query, so a phone that loses signal at 80% continues rather than restarts.

Multipart is already most of the way to resumable: `ListParts` is that query, as long as the client kept the upload ID. What it doesn't do is resume within a part — a part that fails is re-sent whole.

Our own version is resumable only for as long as the URLs last: every part URL is signed up front with a one hour expiry and there is no endpoint to sign fresh ones, so an upload resumed after an hour is dead.

## Integrity

Streaming and checking the bytes pull against each other, and we chose streaming twice in `storage.go`:

- The SDK computes a CRC32 of the payload by default so S3 can reject a corrupted upload. Computing it means having all the bytes, so the stream gets buffered to make it possible. `RequestChecksumCalculationWhenRequired` leaves it off unless an operation demands one, and `PutObject` does not.
- SigV4 signs a hash of the body, which has the same problem, so the body is left out of the signature (the request itself is still signed).

So nothing verifies the bytes in transit. `CompleteMultipartUpload` does reject a manifest whose ETags do not match the parts the store holds, but that checks the assembly, not the transmission: a part corrupted on the way is stored corrupted, its ETag is computed over the corrupt bytes, and the manifest matches. The tradeoff is deliberate, but it is a tradeoff.

The other trap is the ETag. For a multipart object it is **not** the MD5 of the object — it's `md5(concat of the part MD5s)-N`, with the part count on the end. Comparing it to a local MD5 will never match.

## MinIO

Abandoned multipart uploads leak invisible parts — they hold disk (on S3, a bill) and never show up in a listing. On S3 the fix is a lifecycle rule, `AbortIncompleteMultipartUpload: {DaysAfterInitiation: 1}`. This MinIO release rejects that rule with `InvalidArgument`, but it cleans them up on its own with a server-level knob instead, already at the equivalent default:

```bash
mc admin config get local api
# ... stale_uploads_expiry=24h stale_uploads_cleanup_interval=6h ...
```

The admin console is at http://localhost:9001 — user `minioadmin`, password `minioadmin`.

## Examples

### Store

```go
type Config struct {
  Endpoint string
  // PublicEndpoint is used when we want direct upload to the bucket and we are not in
  // the same network as the object storage.
  PublicEndpoint string
  Bucket         string
  AccessKey      string
  SecretKey      string
}

type Store interface {
  // Ping can fail if store is unreachable or these credentials cannot see this bucket.
  Ping() error
  Put(key, contentType string, size int64, body io.Reader) error
  CreateMultipartUpload(key, contentType string) (string, error)
  PresignUploadPart(key, uploadID string, partNumber int32, expires time.Duration) (string, error)
  CompleteMultipartUpload(key, uploadID string, parts []Part) error
}
```

### Through the service + Raw body + `Content-Length`

```go
func (c *Client) uploadFile(path, contentType string) {
  file, err := os.Open(path)
  info, err := file.Stat()
  size := info.Size()

  req, err := http.NewRequest("PUT", "http://localhost:8000/upload-file", file)
  req.Header.Set("Content-Type", contentType)
  // net/http does not infer ContentLength from a file.
  req.ContentLength = size
  res, err := c.Do(req)
}
```

```http
PUT /upload-file HTTP/1.1
Host: localhost:8000
User-Agent: Go-http-client/1.1
Content-Length: 594
Content-Type: application/octet-stream
Accept-Encoding: gzip

package main
<More code>
}
```

```go
func (s *Server) uploadFile(w http.ResponseWriter, r *http.Request) {
  // Check `r.ContentLength`.
  contentType := r.Header.Get("Content-Type")
  body := http.MaxBytesReader(w, r.Body, maxBytes)
  err := s.Store.Put("fileKey", contentType, r.ContentLength, body)
}
```

### Through the service + `multipart/form-data`

```go
func (c *Client) uploadFile() {
  var buf bytes.Buffer
  w := multipart.NewWriter(&buf)
  w.WriteField("username", "Daniel")

  fw, err := w.CreateFormFile("file", "main.go")
  f, err := os.Open("main.go")
  defer f.Close()
  io.Copy(fw, f)

  w.Close()

  req, err := http.NewRequest("POST", "http://localhost:8000/upload-file", &buf)
  req.Header.Set("Content-Type", w.FormDataContentType())
  res, err := c.Do(req)
}
```

```http
POST /upload-file HTTP/1.1
Host: localhost:8000
User-Agent: Go-http-client/1.1
Content-Length: 904
Content-Type: multipart/form-data; boundary=7bc0f8bc7d53fb6a6768f4a9c47a6d1520cf8314402524d29378da0a54b0
Accept-Encoding: gzip

--7bc0f8bc7d53fb6a6768f4a9c47a6d1520cf8314402524d29378da0a54b0
Content-Disposition: form-data; name="username"

Daniel
--7bc0f8bc7d53fb6a6768f4a9c47a6d1520cf8314402524d29378da0a54b0
Content-Disposition: form-data; name="file"; filename="main.go"
Content-Type: application/octet-stream

package main
<More code>
}

--7bc0f8bc7d53fb6a6768f4a9c47a6d1520cf8314402524d29378da0a54b0--
```

### Direct to bucket + S3 multipart upload

```go
func (c *Client) uploadFile(path, contentType string) {
  // Get file that extends `io.Reader` and size.
  file, err := os.Open(path)
  info, err := file.Stat()
  size := info.Size()

  begin, err := c.Post("/begin-file-upload", Request{contentType, size})

  parts := make([]Part, len(begin.Parts))
  for i, part := range begin.Parts {
    // Offset and size of this part. Last part is usually smaller.
    offset := int64(i) * begin.PartSizeBytes
    length := min(begin.PartSizeBytes, size-offset)

    body := io.NewSectionReader(file, offset, length)
    req, err := http.NewRequest("PUT", part.URL, body)
    // net/http does not infer ContentLength from a SectionReader.
    req.ContentLength = length
    res, err := c.Do(req)

    // Etag is a unique digital fingerprint for a specific version of a web resource.
    etag := res.Header.Get("ETag")
    parts[i] = Part{Number: part.Number, ETag: etag}
  }

  _, err = c.Post("/complete-file-upload", Request{begin.UploadID, parts})
}
```

```go
func (s *Server) beginFileUpload(w http.ResponseWriter, r *http.Request) {
  var req Request
  err := json.NewDecoder(r.Body).Decode(&req)
  size := req.SizeBytes

  // Check `size`.
  // If the user sends an invalid size then he will receive less or more parts than
  // necessary and the upload will fail.
  uploadID, err := s.Store.CreateMultipartUpload("fileKey", req.ContentType)

  partCount := (size + partSize - 1) / partSize
  parts := make([]PartURL, partCount)
  for i := range parts {
    // Part numbers are 1-based, which is S3's convention.
    number := int32(i + 1)
    url, err := s.Store.PresignUploadPart("fileKey", uploadID, number, lifetime)
    parts[i] = PartURL{Number: number, URL: url}
  }

  w.Header().Set("Content-Type", "application/json")
  err = json.NewEncoder(w).Encode(Response{
    UploadID:      uploadID,
    Parts:         parts,
    PartSizeBytes: partSize,
    ExpiresAt:     time.Now().Add(lifetime),
  })
}

func (s *Server) completeFileUpload(w http.ResponseWriter, r *http.Request) {
  var req Request
  err := json.NewDecoder(r.Body).Decode(&req)
  err = s.Store.CompleteMultipartUpload("fileKey", req.UploadID, req.Parts)
}
```
