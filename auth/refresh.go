package main

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net/http"
	"time"
)

const (
	// refreshCookiePath is the whole of why this is a cookie at all.
	//
	// Scoped to the one endpoint that consumes it, the browser will not attach
	// it to a request for anything else — so the long-lived credential is
	// absent from every ordinary API call and cannot be leaked by one. The
	// short-lived access token travels on those instead, in a header the client
	// sets deliberately.
	refreshCookiePath = "/identity/refresh"
	refreshCookieName = "refresh_token"

	refreshTokenBytes = 32

	// Long, because it is the only thing standing between the user and logging
	// in again, and it is safe to be long precisely because it is rotated on
	// every use and revocable on reuse. A stolen one is worth a single refresh
	// before the theft is detected.
	refreshLifetime = 30 * 24 * time.Hour
)

// newRefreshToken returns a fresh opaque token. 32 bytes from crypto/rand is
// 256 bits of entropy, so the token is not guessable and needs no other
// structure.
//
// Opaque and not a JWT, deliberately. A signed refresh token could be checked
// without a lookup, which sounds like the same win the access token gets — but
// it would also be unrevocable, and revocation is the entire job of this half
// of the pair.
func newRefreshToken() (string, error) {
	b := make([]byte, refreshTokenBytes)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generating refresh token: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// hashRefreshToken is what gets stored, so a leaked database dump cannot be
// replayed as live logins.
//
// SHA-256 rather than Argon2id on purpose: the token is 256 random bits, so
// there is no guessable password to slow an attacker down.
func hashRefreshToken(t string) []byte {
	sum := sha256.Sum256([]byte(t))
	return sum[:]
}

// refreshCookie issues the refresh cookie. HttpOnly keeps it out of reach of
// JavaScript, so an XSS cannot read it; SameSite=Strict means no cross-site
// request carries it at all, which is what removes the need for a CSRF token
// on the one endpoint that accepts it.
func refreshCookie(cfg Config, value string, expiresAt time.Time) http.Cookie {
	return http.Cookie{
		Name:     refreshCookieName,
		Value:    value,
		Path:     refreshCookiePath,
		Expires:  expiresAt,
		HttpOnly: true,
		Secure:   cfg.CookieSecure,
		SameSite: http.SameSiteStrictMode,
	}
}

// expiredRefreshCookie deletes the cookie on the client via a negative MaxAge.
// Path has to match the one it was set with or the browser will clear nothing.
func expiredRefreshCookie(cfg Config) http.Cookie {
	return http.Cookie{
		Name:     refreshCookieName,
		Value:    "",
		Path:     refreshCookiePath,
		MaxAge:   -1,
		HttpOnly: true,
		Secure:   cfg.CookieSecure,
		SameSite: http.SameSiteStrictMode,
	}
}

// TokenResponse is what both /identity/login and /identity/refresh return. The
// access token goes in the body rather than a cookie so a browser client can
// hold it in memory and attach it as a header: nothing on disk, nothing
// readable by JavaScript from another tab, and nothing sent automatically.
type TokenResponse struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int    `json:"expires_in"`
}

func newTokenResponse(accessToken string, expiresAt time.Time) TokenResponse {
	return TokenResponse{
		AccessToken: accessToken,
		TokenType:   "Bearer",
		ExpiresIn:   int(time.Until(expiresAt).Seconds()),
	}
}

// grantedScopes is what a login buys. Every account gets the same set for now;
// when accounts start to differ this is the one place that has to know, and
// nothing downstream changes because the scope is already in the token.
//
// Worth knowing: a scope taken away here is still in every access token
// already issued, so it stops applying only when those expire. That is the
// price of not asking anyone on the read path, and the access token lifetime
// is the knob that sets it.
var grantedScopes = []string{ScopeOrdersRead}
