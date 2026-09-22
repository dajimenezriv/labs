package main

import (
	"crypto/ed25519"
	"fmt"
)

type Config struct {
	CookieSecure  bool
	Port          int
	JWTPrivateKey string
}

// SigningKey decodes the configured key. It is a method rather than a parsed
// field because env only fills in strings, and a key of the wrong length
// should stop the process here rather than at the first login.
func (c Config) SigningKey() (ed25519.PrivateKey, error) {
	key, err := ParsePrivateKey(c.JWTPrivateKey)
	if err != nil {
		return nil, fmt.Errorf("JWT_PRIVATE_KEY: %w", err)
	}
	return key, nil
}
