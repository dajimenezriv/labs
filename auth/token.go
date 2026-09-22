// Package token is the access-token contract, shared by everything that mints
// or checks one.
//
// It is one package rather than a copy per service because both halves have to
// agree exactly: identity mints, the rest verify, and a disagreement about the
// issuer, the audience or the signing method surfaces only as a 401 nobody can
// explain. Keeping the key material split is what keeps the trust split honest
// — an Issuer needs the private key, a Verifier never sees anything but the
// public one, so a service that only checks tokens cannot forge one.
//
// Every token names the key that signed it in its kid header, and verifiers
// resolve that name through a Keys. That indirection is what makes rotation an
// ordinary deploy rather than a flag day: publish the new public key, wait for
// the verifiers to see it, then start signing with it, then drop the old one
// once the last token signed with it has expired. Nothing has to happen
// simultaneously, so nothing has to be coordinated.
package main

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

const (
	// issuerName is the identity service: the only thing in the estate holding
	// a private key, so the only issuer a verifier will accept.
	issuerName = "identity"

	// audienceName names the trust boundary rather than one service.
	//
	// A token is minted at the edge and then travels inward — the shop hands
	// the same one to the carrier — so there is no single recipient to put in
	// the claim. The cost is that a token lifted from one internal service can
	// be replayed against another; closing that means re-exchanging the token
	// at every hop, which buys little while every hop is on one private
	// network.
	audienceName = "internal"
)

// ScopeOrdersRead is what looking up a shipment demands. Scopes are the
// authorization half: the signature says who the caller is, the scope says
// what they may ask for.
const ScopeOrdersRead = "orders:read"

// signingMethod is EdDSA (Ed25519), pinned for both signing and verifying.
//
// Pinning is what stops alg confusion: a token that says "alg":"none", or one
// that says HMAC hoping the verifier will use the public key as a shared
// secret. Here it is the second line of defence rather than the first, since
// the key the keyfunc hands back is an ed25519.PublicKey and neither of those
// methods will accept one. It is stated anyway because that is a property of
// the keyfunc, not a decision, and the day the keyfunc returns a key set
// instead this is the only thing still holding.
var signingMethod = jwt.SigningMethodEdDSA

// Claims is the token payload. Scope is a space-delimited list, which is what
// OAuth 2.0 uses and what most tooling expects to find.
type Claims struct {
	jwt.RegisteredClaims
	Scope string `json:"scope"`
}

func (c *Claims) HasScope(want string) bool {
	return slices.Contains(strings.Fields(c.Scope), want)
}

// UserID reads the subject back as a uuid. The subject is a string in the
// spec, so this is where the token stops being text and starts being a user.
func (c *Claims) UserID() (uuid.UUID, error) {
	id, err := uuid.Parse(c.Subject)
	if err != nil {
		return uuid.Nil, fmt.Errorf("parse subject: %w", err)
	}
	return id, nil
}

// KeyID names a key by its own bytes: the base64url SHA-256 of the public half.
//
// Derived rather than configured on purpose. A key id has to be agreed on by
// the signer and every verifier, and anything written down twice eventually
// disagrees; this way both sides compute the same name from the same key and
// there is no third thing to keep in step.
func KeyID(pub ed25519.PublicKey) string {
	sum := sha256.Sum256(pub)
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

// Issuer mints access tokens. It holds the private key.
type Issuer struct {
	key      ed25519.PrivateKey
	kid      string
	lifetime time.Duration
}

func NewIssuer(key ed25519.PrivateKey, lifetime time.Duration) *Issuer {
	return &Issuer{
		key:      key,
		kid:      KeyID(key.Public().(ed25519.PublicKey)),
		lifetime: lifetime,
	}
}

// KeyID is the name this issuer signs under, which is what its JWKS has to
// publish for anything it mints to be verifiable.
func (i *Issuer) KeyID() string { return i.kid }

// PublicKey is the half that is safe to hand out, and the only half anything
// else in the estate ever needs.
func (i *Issuer) PublicKey() ed25519.PublicKey {
	return i.key.Public().(ed25519.PublicKey)
}

// Issue returns a signed token for the user and the moment it expires, so the
// caller can say how long it has without parsing it back.
func (i *Issuer) Issue(userID uuid.UUID, scopes []string) (string, time.Time, error) {
	now := time.Now()
	expiresAt := now.Add(i.lifetime)

	t := jwt.NewWithClaims(signingMethod, &Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   userID.String(),
			Issuer:    issuerName,
			Audience:  jwt.ClaimStrings{audienceName},
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(expiresAt),
			ID:        uuid.NewString(),
		},
		Scope: strings.Join(scopes, " "),
	})

	// In the header rather than the claims because a verifier has to read it
	// before it can check the signature, and at that point the claims are
	// nothing but unverified bytes.
	t.Header["kid"] = i.kid

	signed, err := t.SignedString(i.key)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("sign token: %w", err)
	}

	return signed, expiresAt, nil
}

// Keys resolves the kid in a token header to the key that signed it. It is an
// interface so that a verifier does not care whether the answer came from a
// constant, a file or an HTTP fetch — StaticKeys and RemoteKeys are the two
// that exist.
type Keys interface {
	Key(ctx context.Context, kid string) (ed25519.PublicKey, error)
}

// StaticKeys is one key that never changes. It is what tests use, and what a
// service would use if it were handed a public key directly rather than
// fetching a set.
type StaticKeys map[string]ed25519.PublicKey

// NewStaticKeys names each key the same way an issuer does, so a caller only
// has to hold the keys and never the ids.
func NewStaticKeys(pubs ...ed25519.PublicKey) StaticKeys {
	keys := make(StaticKeys, len(pubs))
	for _, pub := range pubs {
		keys[KeyID(pub)] = pub
	}
	return keys
}

func (s StaticKeys) Key(_ context.Context, kid string) (ed25519.PublicKey, error) {
	key, ok := s[kid]
	if !ok {
		return nil, fmt.Errorf("unknown key id %q", kid)
	}
	return key, nil
}

// Verifier checks access tokens. It only ever sees public keys, so leaking its
// configuration does not let anyone forge one.
type Verifier struct {
	keys Keys
}

func NewVerifier(keys Keys) *Verifier {
	return &Verifier{keys: keys}
}

// Verify returns the claims of a valid token. Every failure is one error to
// the caller: which check failed is useful in a log, never in a response.
//
// The context is here because resolving a kid may be a network call the first
// time a key is seen; it is a cache hit on every request after that.
func (v *Verifier) Verify(ctx context.Context, raw string) (*Claims, error) {
	var c Claims

	_, err := jwt.ParseWithClaims(raw, &c,
		func(t *jwt.Token) (any, error) {
			kid, ok := t.Header["kid"].(string)
			if !ok {
				return nil, errors.New("token has no kid header")
			}
			return v.keys.Key(ctx, kid)
		},
		jwt.WithValidMethods([]string{signingMethod.Alg()}),
		jwt.WithIssuer(issuerName),
		jwt.WithAudience(audienceName),
		// Without this a token that simply omits exp would never expire.
		jwt.WithExpirationRequired(),
	)
	if err != nil {
		return nil, fmt.Errorf("parse token: %w", err)
	}

	return &c, nil
}

// ParsePrivateKey decodes one base64 ed25519 signing key and checks its
// length, since a key of the wrong size otherwise fails much later, at the
// first attempt to sign.
//
// There is no public counterpart any more: nothing is configured with a public
// key now that they are published as a JWKS and fetched.
func ParsePrivateKey(encoded string) (ed25519.PrivateKey, error) {
	if encoded == "" {
		return nil, errors.New("key is empty (generate a pair with: go run ./cmd/jwtkeys)")
	}

	b, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("key must be base64: %w", err)
	}
	if len(b) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("key must decode to %d bytes, got %d", ed25519.PrivateKeySize, len(b))
	}

	return ed25519.PrivateKey(b), nil
}
