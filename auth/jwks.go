package main

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// JWK is one public key in the form RFC 8037 gives Ed25519: an octet key pair
// ("OKP") on the Ed25519 curve, with the key itself base64url in x.
//
// There is nothing secret in it. That is the point of publishing it over plain
// HTTP with no authentication: a public key is worth nothing to a forger, and
// requiring a credential to fetch one would mean needing a credential in order
// to check a credential.
type JWK struct {
	Kty string `json:"kty"`
	Crv string `json:"crv"`
	X   string `json:"x"`
	Kid string `json:"kid"`
	Use string `json:"use"`
	Alg string `json:"alg"`
}

// JWKS is the document served at JWKSPath. It is a list rather than one key
// because a rotation needs both the outgoing and the incoming key to be
// resolvable at once — a token signed a minute ago is still valid.
type JWKS struct {
	Keys []JWK `json:"keys"`
}

func NewJWKS(pubs ...ed25519.PublicKey) JWKS {
	keys := make([]JWK, 0, len(pubs))
	for _, pub := range pubs {
		keys = append(keys, JWK{
			Kty: "OKP",
			Crv: "Ed25519",
			X:   base64.RawURLEncoding.EncodeToString(pub),
			Kid: KeyID(pub),
			Use: "sig",
			Alg: signingMethod.Alg(),
		})
	}
	return JWKS{Keys: keys}
}

// keys decodes the document into something a verifier can look up in.
//
// A key of the wrong type or curve is skipped rather than fatal: the set is
// allowed to grow a key this verifier does not understand, and refusing the
// whole document over one unknown entry would turn a forward-compatible change
// into an outage.
func (s JWKS) keys() (map[string]ed25519.PublicKey, error) {
	out := make(map[string]ed25519.PublicKey, len(s.Keys))

	for _, k := range s.Keys {
		if k.Kty != "OKP" || k.Crv != "Ed25519" {
			continue
		}

		b, err := base64.RawURLEncoding.DecodeString(k.X)
		if err != nil {
			return nil, fmt.Errorf("key %q: x must be base64url: %w", k.Kid, err)
		}
		if len(b) != ed25519.PublicKeySize {
			return nil, fmt.Errorf("key %q: x must decode to %d bytes, got %d", k.Kid, ed25519.PublicKeySize, len(b))
		}

		out[k.Kid] = ed25519.PublicKey(b)
	}

	return out, nil
}

const (
	// How long to wait before fetching again after any attempt, successful or
	// not.
	//
	// This is the only thing standing between a verifier and being used as an
	// amplifier: without it, a stream of tokens carrying random kids would
	// become one request to identity each. With it that stream costs one
	// request per window no matter how fast it arrives.
	//
	// It is short because it is also the ceiling on how long a rotation takes
	// to be noticed. Long enough to blunt the flood, short enough that the new
	// key is picked up in seconds rather than minutes.
	minRefetchInterval = 10 * time.Second

	// A fetch is on the critical path of a request that found an unknown kid,
	// so it may not take longer than the caller is prepared to wait.
	fetchTimeout = 5 * time.Second

	// The document is a handful of 32-byte keys. Anything approaching this is
	// not a key set.
	maxJWKSBytes = 1 << 20
)

// RemoteKeys resolves key ids against a JWKS endpoint, caching what it finds.
//
// The cache is what keeps verification local: after the first token, checking
// a signature is CPU and nothing else, which is the property that made a
// signed token worth having over a lookup. The fetch exists only for the case
// the cache cannot answer — a kid it has never seen, which in practice means a
// key that was rotated in after this process started.
type RemoteKeys struct {
	url  string
	http *http.Client

	// fetchMu serializes fetches so that N concurrent requests carrying a new
	// kid make one call and not N. It is separate from mu because it is held
	// across the network call, and readers must not block on that.
	fetchMu sync.Mutex

	mu          sync.RWMutex
	keys        map[string]ed25519.PublicKey
	lastAttempt time.Time
}

func (r *RemoteKeys) cached(kid string) (ed25519.PublicKey, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	key, ok := r.keys[kid]
	return key, ok
}

// Key returns the public key for kid, fetching the set if it has not seen it.
//
// An unknown kid after a fetch is reported as unknown rather than retried: it
// means the token was signed by something this estate does not publish a key
// for, which is either a forgery or a key that was withdrawn, and both should
// end as a failed verification.
func (r *RemoteKeys) Key(ctx context.Context, kid string) (ed25519.PublicKey, error) {
	if key, ok := r.cached(kid); ok {
		return key, nil
	}

	if err := r.refresh(ctx); err != nil {
		return nil, err
	}

	if key, ok := r.cached(kid); ok {
		return key, nil
	}
	return nil, fmt.Errorf("unknown key id %q", kid)
}

// refresh replaces the cached set, at most once per minRefetchInterval.
//
// Being throttled is not an error: the caller still has whatever was cached
// before, and reporting a failure here would turn a rate limit of ours into a
// 500 for a request that is about to fail as an unknown kid anyway.
func (r *RemoteKeys) refresh(ctx context.Context) error {
	r.fetchMu.Lock()
	defer r.fetchMu.Unlock()

	r.mu.RLock()
	tooSoon := time.Since(r.lastAttempt) < minRefetchInterval
	r.mu.RUnlock()
	if tooSoon {
		return nil
	}

	// Recorded before the call rather than after, so a fetch that hangs until
	// the timeout still counts as an attempt and cannot be repeated by the
	// next caller the moment it fails.
	r.mu.Lock()
	r.lastAttempt = time.Now()
	r.mu.Unlock()

	ctx, cancel := context.WithTimeout(ctx, fetchTimeout)
	defer cancel()

	keys, err := r.fetch(ctx)
	if err != nil {
		return err
	}

	r.mu.Lock()
	r.keys = keys
	r.mu.Unlock()

	return nil
}

func (r *RemoteKeys) fetch(ctx context.Context) (map[string]ed25519.PublicKey, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.url, nil)
	if err != nil {
		return nil, fmt.Errorf("new request: %w", err)
	}

	res, err := r.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("get %s: %w", r.url, err)
	}
	defer func() { _ = res.Body.Close() }()

	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("get %s: %s", r.url, res.Status)
	}

	var doc JWKS
	if err := json.NewDecoder(io.LimitReader(res.Body, maxJWKSBytes)).Decode(&doc); err != nil {
		return nil, fmt.Errorf("decode jwks: %w", err)
	}

	keys, err := doc.keys()
	if err != nil {
		return nil, fmt.Errorf("read jwks: %w", err)
	}
	if len(keys) == 0 {
		// Replacing a working set with an empty one would fail every request
		// from here on, and an empty document is far more likely to be a
		// misconfigured endpoint than a genuine statement that nothing signs.
		return nil, fmt.Errorf("get %s: no usable keys", r.url)
	}

	return keys, nil
}
