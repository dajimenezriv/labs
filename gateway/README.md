# Gateway

What happens if we close a container and open it again, then it gets a new IP, how the proxy still connects to that service?

Do: TLS termination, host/path routing to upstreams, timeouts, request body size limits, rate limiting, gzip, access logs, injecting a request ID.

Sticky sessions allow us to forward requests from a user to the same backend. This allow the use of websockets, also if the backend stores some data in memory.

Alternatives: Caddy and Traefik.

What is a nginx worker?
How to keep traces, adding a X-Request-ID that is used in all requests and logger?
I assume that Traefik basically tries to access the healthz endpoints of the containers to see what's alive and see where to send?

Why it seems that the X-Forwarded headers are so important?

We need to also make timeout to connections. Go doesn't impose a max number of connections, but the system maybe does.
- File decriptors: 1 per connection. `ulimit -n` is often 1024 by default. We need to close connections, otherwise that's how you leak descriptors until accept: "too many open files".

An HTTP/1.1 browser opens up to ~6 connections per host, so 5k browser users can be 30k sockets. HTTP/2 collapses that back to 1 per user.

## Introduction

- **Reverse proxy**: sits in front of servers and forwards requests on their behalf.
- **Load balancer**: distributes traffic across multiple instances of the same service. Two types, working on L4 (TCP), it picks a backend and sends bytes, or L7 (HTTP), which lets route on path and header, retry idempotent requests and report per-endpoint metrics.
- **API gateway**: knows about hosts, paths and status codes. What a user is, a tenant is, an API key is. Can authenticate and authorize requests, per-consumer quotas rather than per-IP limits, etc.

## Reverse Proxy

A Reverse Proxy is not a pipe, we don't forward a connection, we create a new one with our service. 

```go
type proxy struct {
	target    *url.URL
	transport http.RoundTripper
}

// We receive a connection from the client.
func ServeHTTP(w http.ResponseWriter, r *http.Request) {
  // We copy the request from the client.
  out := r.Clone(r.Context())
  // Change the target URL to our own service.
  out.URL.Scheme = p.target.Scheme
  out.URL.Host = p.target.Host
  // Parse headers.
  removeHopByHopHeaders(out.Header)
  setForwardedHeaders()
  // Perform request.
  res, _ := p.transport.RoundTrip(out)
  // Parse headers.
  removeHopByHopHeaders(res.Header)
  dst := w.Header()
  // Use our parsed headers.
  maps.Copy(dst, res.Header)
  // Send response
  w.WriteHeader(res.StatusCode)
  io.Copy(w, res.Body)
}
```

### HTTP headers

HTTP headers let the client and server pass additional information with a message in a request or response.

Can be grouped acording to their contexts:

- **Request headers**: information about the resource to be fetched or the client requesting the resource (Ex: `Authorization`).
- **Response headers**: information about the response, like its location or the server providing it (Ex: `Age`).
- **Representation headers**: information about the body of the resource, like its MIME type or encoding/compression applied (Ex: `Content-Type`).
- **Payload headers**: information about payload data, including content length and the encoding used for transport (Ex: `Content-Length`).

Can be grouped acording to how proxies handle them:

- **End-to-end headers**: must be transmitted to the final recipent of the message.
- **Hop-by-hop headers**: just useful for a single transport-level connection, must not be retransmitted. There are eight standard (`Connection`, `Keep-Alive`, `Transfer-Encoding`, `TE`, `Trailer`, `Upgrade`, `Proxy-Authenticate`, `Proxy-Authorization`). Here we can see the Go list of hop-by-hop headers: https://github.com/golang/go/blob/master/src/net/http/httputil/reverseproxy.go#L381.

`Connection` contains a list of connection options, and order doesn't matter. Each token is one of:

- **close**: a defined option meaning "I'll close this connection after this message".
- **keep-alive**: redundant in HTTP/1.1, where persistence is the default, but sent constantly anyway.
- **any header field name**: meaning "that header is hop-by-hop on this connection, consume it".

So all of these are valid and equivalent in structure:

```
Connection: close
Connection: keep-alive
Connection: Upgrade
Connection: keep-alive, X-My-Internal-Token
Connection: X-My-Internal-Token, close
```

All these hop-by-hop headers values are applied automatically to the connection except the `Upgrade` header that we must handle. In Go Reverse Proxy is handled here: https://github.com/golang/go/blob/master/src/net/http/httputil/reverseproxy.go#L836.

### Core

```nginx
upstream identity {
  server host.docker.internal:3000;
  keepalive 32;
}

location = /identity/login {
  limit_req zone=per_ip_auth burst=5;
  limit_conn conns_per_ip 20;
  proxy_pass http://identity;
}
```

#### Forward one route to the backend

We need to forward a route to the specific service that handles it.

`https://reverse-proxy/identity/login` → `http://identity-ip:3000/`.

About the `X-Forwarded` headers: `X-Forwarded-For` means "hey backend, I know your socket says the client is me, but the real one was 203.0.113.9." X-Forwarded-Proto: https means "your socket is plaintext, but the user is on HTTPS." For example, we want to pass the clientID in case we want to implement rate limiting based on IP.

`X-Real-IP` holds a single IP address, while `X-Forwarded-For` keeps a full, comma-separated list of every proxy the request passed through

We also process headers:

- `Host`: Go `httputil.ReverseProxy` keeps the inbound Host by default.
- `X-Forwarded-For`:
- Hop-by-hop headers (`Connection`, `Keep-Alive`, `Transfer-Encoding`, `Upgrade`, `Proxy-*`) They belong to the previous connection.

## Route table

- If we have N instances of a service, decide to which instance it goes.
- Decides what do based on the route:
  - Longest prefix wins: `/api/users` must beat `/api` when both are registered.
  - Exact beats prefix.
  - Prefix stripping: send `/api/users/42` or `/users/42`.
  - 404 ownership: no route matched.

Forward one route to one backend. httputil.ReverseProxy plus a Director or Rewrite func. Immediately hit the header question — Host, X-Forwarded-For, X-Forwarded-Proto — and decide what you forward and what you strip.
Route table. Path prefix → backend. Now you own the matching semantics: longest prefix wins, exact beats prefix, and what a 404 at the gateway means versus one from a service.
Backend pool + load balancing. Multiple instances behind one route. Implement round-robin, then least-connections, and generate uneven latency across your backends so you can see round-robin do badly.
Health checks. Active (you poll /healthz) and passive (you notice failures and eject). Then deliberately break the shared database so every backend fails its check, and watch your gateway take the whole system down — that failure mode is the single most valuable thing on this list to have experienced.
Timeouts and cancellation. Per-route timeouts, and make sure context cancellation actually propagates so an abandoned client stops work in the backend rather than leaving it running.
Retries. Only for idempotent methods, with backoff and jitter, and with a budget — a cap on retries as a fraction of total traffic. Without the budget you've built a retry storm generator.
Circuit breaker. Stop sending to a backend that's failing, probe occasionally to see if it recovered.
Graceful shutdown. Stop accepting, drain in-flight, exit. Test it by deploying under load and confirming zero dropped requests.
Observability. Request ID injection, propagate trace context, RED metrics per route. This plugs straight into the Grafana stack you're already learning.
