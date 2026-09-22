package main

import (
	"errors"
	"io"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"net/url"
	"strings"
)

// hopByHop are the headers that describe one leg of a connection rather than
// the message travelling over it. They are meaningful between two adjacent
// participants and meaningless to the next hop, so a proxy consumes them
// instead of passing them on — forwarding Connection: keep-alive would be
// telling the upstream about a connection it is not part of.
//
// RFC 9110 §7.6.1. Proxy-Connection was never standardised but is still emitted
// by old clients, and it causes the same confusion, so it goes too.
var hopByHop = []string{
	"Connection",
	"Keep-Alive",
	"Proxy-Authenticate",
	"Proxy-Authorization",
	"Proxy-Connection",
	"TE",
	"Trailer",
	"Transfer-Encoding",
	"Upgrade",
}

// maxBodyBytes is the largest request body the gateway will pass on by
// default: nginx's client_max_body_size 1m. Big enough for the JSON every
// route but one is made of.
const maxBodyBytes = 1 << 20
const maxAvatarBytes = 5 << 20

// proxy forwards everything it is given to one origin.
//
// The routing decision has already been made by the time a request arrives
// here: a proxy knows its target and nothing about the others.
type proxy struct {
	target    *url.URL
	transport http.RoundTripper
	log       *slog.Logger
	maxBody   int64
}

func (p *proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// This is done in nginx with client_max_body_size.
	// We are doing 2 checks, if we already know the ContentLength is bigger than
	// maxBody we stop the request, however when we have a chunked body we don't
	// know the ContentLength in advance, so we need to use http.MaxBytesReader to
	// enforce the cap while reading.
	if r.ContentLength > p.maxBody {
		requestEntityTooLarge(w)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, p.maxBody)

	// A server request and a client request are the same struct read under
	// different rules, and this is where one becomes the other.
	//
	// Clone deep-copies the headers, so editing them below cannot reach back
	// into the request the server is still holding. It carries the context with
	// it, which is what makes an abandoned client cancel the upstream call
	// rather than leave it running.
	out := r.Clone(r.Context())

	// Server-side, RequestURI is the raw target line and URL is parsed from it.
	// Client-side the transport reads URL and refuses any request with
	// RequestURI set — the one field that must be cleared rather than filled.
	out.RequestURI = ""
	out.URL.Scheme = p.target.Scheme
	out.URL.Host = p.target.Host

	// out.Host is left as the caller sent it. The transport prefers Host over
	// URL.Host when writing the Host header, so the upstream sees the name the
	// client asked for rather than its own address — nginx's `proxy_set_header
	// Host $host`. Services that build absolute URLs need it to stay that way.

	removeHopByHop(out.Header)
	setForwardedHeaders(out, r)

	res, err := p.transport.RoundTrip(out)
	if err != nil {
		// A cancelled context means the client hung up, so there is nobody left
		// to answer and writing a status would only log a confusing one.
		if errors.Is(err, r.Context().Err()) {
			return
		}

		// The one failure to forward a request that is not the upstream's
		// fault: an undeclared body ran past the cap above and the read that
		// fed the transport is what failed. 502 would blame the wrong party.
		//
		// Some of it has already reached the upstream by now — this is what
		// the Content-Length check avoids paying, and all it can do for a
		// body whose length nobody declared.
		if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
			requestEntityTooLarge(w)
			return
		}

		p.log.ErrorContext(r.Context(), "upstream", "target", p.target.String(), "path", r.URL.Path, "err", err)
		http.Error(w, `{"error":"bad gateway"}`, http.StatusBadGateway)

		return
	}
	// Draining is what returns the connection to the pool; closing an unread
	// body throws the connection away instead, and the keepalives above with it.
	defer func() { _ = res.Body.Close() }()

	// The response gets the same treatment in reverse: the upstream's
	// connection headers describe its link to us, not ours to the client.
	removeHopByHop(res.Header)

	dst := w.Header()
	maps.Copy(dst, res.Header)

	// Order matters from here: headers must be set before WriteHeader, and
	// WriteHeader before the first write of the body.
	w.WriteHeader(res.StatusCode)

	// Streams the body rather than buffering it, so a large response does not
	// have to fit in memory. Anything already flushed to the client cannot be
	// taken back, which is why a mid-copy failure can only be logged.
	//
	// Note this does not flush as it goes: a server-sent-events endpoint behind
	// this would be buffered rather than live. Nothing here streams today.
	if _, err := io.Copy(w, res.Body); err != nil {
		p.log.WarnContext(r.Context(), "copy body", "target", p.target.String(), "path", r.URL.Path, "err", err)
	}
}

func requestEntityTooLarge(w http.ResponseWriter) {
	http.Error(w, `{"error":"request entity too large"}`, http.StatusRequestEntityTooLarge)
}

// removeHopByHop strips the connection-scoped headers from h.
func removeHopByHop(h http.Header) {
	// Connection names the other headers that are hop-by-hop for this message,
	// so it has to be read before it is deleted. This is the part that is easy
	// to miss: the fixed list above is only half of the rule.
	for _, value := range h.Values("Connection") {
		for name := range strings.SplitSeq(value, ",") {
			if name = strings.TrimSpace(name); name != "" {
				h.Del(name)
			}
		}
	}

	for _, name := range hopByHop {
		h.Del(name)
	}
}

// setForwardedHeaders tells the upstream what the gateway can see and it
// cannot: who the caller actually was, and how they reached the edge.
func setForwardedHeaders(out, in *http.Request) {
	clientIP := remoteIP(in)

	// Appended, not set: X-Forwarded-For is the chain of every proxy the request
	// has crossed, and overwriting it would erase whatever sat in front of us.
	// Everything except the address we observed ourselves is a claim by someone
	// upstream of us, and worth exactly as much as that party is trusted.
	if prior := in.Header.Get("X-Forwarded-For"); prior != "" {
		out.Header.Set("X-Forwarded-For", prior+", "+clientIP)
	} else {
		out.Header.Set("X-Forwarded-For", clientIP)
	}

	out.Header.Set("X-Real-IP", clientIP)

	// TLS terminates at the edge, so every upstream hop is plaintext and only
	// the gateway still knows whether the client used HTTPS.
	scheme := "http"
	if in.TLS != nil {
		scheme = "https"
	}

	out.Header.Set("X-Forwarded-Proto", scheme)
	out.Header.Set("X-Request-ID", requestIDFrom(in.Context()))
}

// remoteIP is the address the gateway observed the request arrive from, with
// the port dropped. RemoteAddr always carries one; a failure here means it did
// not, which is not a shape net/http produces, so the raw value is a better
// guess than an empty one.
//
// This is the only thing about a caller that is not a claim by the caller, so
// it is what both the forwarded headers report and the rate limiter keys on.
func remoteIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}

	return host
}
