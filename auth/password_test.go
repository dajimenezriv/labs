package main

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"testing"

	"golang.org/x/crypto/argon2"
)

func b64(b []byte) string { return base64.RawStdEncoding.EncodeToString(b) }

func TestHashPasswordRoundTrip(t *testing.T) {
	passwords := []string{
		"correct-horse-battery",
		"a",
		"",
		"contains spaces and $dollar$ signs",
		"unicode: 日本語 🔐",
		strings.Repeat("x", 1024),
	}

	for _, password := range passwords {
		t.Run(password, func(t *testing.T) {
			hash, err := hashPassword(password)
			if err != nil {
				t.Fatalf("hashPassword()=%v, want=%v", err, nil)
			}

			ok, err := verifyPassword(password, hash)
			if err != nil {
				t.Fatalf("verifyPassword()=%v, want=%v", err, nil)
			}
			if !ok {
				t.Error("verifyPassword()=false, want a password to verify against its own hash")
			}

			ok, err = verifyPassword(password+"-wrong", hash)
			if err != nil {
				t.Fatalf("verifyPassword()=%v, want=%v", err, nil)
			}
			if ok {
				t.Error("verifyPassword()=true, want a different password not to verify")
			}
		})
	}
}

func TestHashPasswordFormat(t *testing.T) {
	hash, err := hashPassword("correct-horse-battery")
	if err != nil {
		t.Fatalf("hashPassword()=%v, want=%v", err, nil)
	}

	// PHC format: $argon2id$v=19$m=19456,t=2,p=1$<salt>$<key>
	if !strings.HasPrefix(hash, "$argon2id$") {
		t.Errorf("hash=%q, want it to start with %q", hash, "$argon2id$")
	}

	version := fmt.Sprintf("$v=%d$", argon2.Version)
	if !strings.Contains(hash, version) {
		t.Errorf("hash=%q, want it to contain %q", hash, version)
	}

	params := fmt.Sprintf("$m=%d,t=%d,p=%d$", argonMemoryKiB, argonIterations, argonThreads)
	if !strings.Contains(hash, params) {
		t.Errorf("hash=%q, want it to contain %q", hash, params)
	}

	if got, want := len(strings.Split(hash, "$")), 6; got != want {
		t.Errorf("hash fields=%d, want=%d, hash=%q", got, want, hash)
	}
}

// A random salt per call is what stops identical passwords from sharing a
// digest, which would let an attacker spot repeated passwords in a dump.
func TestHashPasswordIsSalted(t *testing.T) {
	const password = "correct-horse-battery"

	first, err := hashPassword(password)
	if err != nil {
		t.Fatalf("hashPassword()=%v, want=%v", err, nil)
	}
	second, err := hashPassword(password)
	if err != nil {
		t.Fatalf("hashPassword()=%v, want=%v", err, nil)
	}

	if first == second {
		t.Error("the same password produced the same digest twice, want a random salt per call")
	}

	// Both must still verify.
	for _, hash := range []string{first, second} {
		ok, err := verifyPassword(password, hash)
		if err != nil {
			t.Fatalf("verifyPassword()=%v, want=%v", err, nil)
		}
		if !ok {
			t.Errorf("verifyPassword(%q)=false, want=true", hash)
		}
	}
}

func TestVerifyPasswordRejectsMalformedHashes(t *testing.T) {
	valid, err := hashPassword("correct-horse-battery")
	if err != nil {
		t.Fatalf("hashPassword()=%v, want=%v", err, nil)
	}

	tests := map[string]string{
		"empty":               "",
		"not phc at all":      "correct-horse-battery",
		"too few fields":      "$argon2id$v=19$m=19456,t=2,p=1$c29tZXNhbHQ",
		"wrong algorithm":     strings.Replace(valid, "$argon2id$", "$argon2i$", 1),
		"bcrypt digest":       "$2a$10$abcdefghijklmnopqrstuv",
		"unparseable version": strings.Replace(valid, "$v=19$", "$v=abc$", 1),
		"unparseable params":  strings.Replace(valid, "m=19456,t=2,p=1", "m=x,t=y,p=z", 1),
		"salt is not base64":  "$argon2id$v=19$m=19456,t=2,p=1$!!!not-base64!!!$c29tZWtleQ",
		"empty salt and key":  "$argon2id$v=19$m=19456,t=2,p=1$$",
	}

	for name, hash := range tests {
		t.Run(name, func(t *testing.T) {
			ok, err := verifyPassword("correct-horse-battery", hash)
			if ok {
				t.Error("verifyPassword()=true, want a malformed hash never to verify")
			}
			if err == nil {
				t.Error("verifyPassword()=nil, want an error rather than a silent mismatch")
			}
		})
	}
}

// An unknown argon2 version must be reported rather than silently recomputed
// with the current one.
func TestVerifyPasswordRejectsIncompatibleVersion(t *testing.T) {
	valid, err := hashPassword("correct-horse-battery")
	if err != nil {
		t.Fatalf("hashPassword()=%v, want=%v", err, nil)
	}

	tampered := strings.Replace(valid, fmt.Sprintf("$v=%d$", argon2.Version), "$v=16$", 1)

	ok, err := verifyPassword("correct-horse-battery", tampered)
	if ok {
		t.Error("verifyPassword()=true, want=false")
	}
	if !errors.Is(err, errIncompatibleArgon2) {
		t.Errorf("verifyPassword()=%v, want=%v", err, errIncompatibleArgon2)
	}
}

// Parameters are read back from the digest, so a hash made with weaker settings
// than the current constants still verifies. Without this, raising the
// parameters would lock out every existing user.
func TestVerifyPasswordUsesEmbeddedParameters(t *testing.T) {
	const password = "correct-horse-battery"

	// Built by hand with deliberately low parameters, unlike the constants.
	salt := []byte("0123456789abcdef")
	key := argon2.IDKey([]byte(password), salt, 1, 8*1024, 1, 32)
	weak := fmt.Sprintf("$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s",
		argon2.Version, 8*1024, 1, 1,
		b64(salt), b64(key))

	if argonMemoryKiB == 8*1024 {
		t.Fatalf("argonMemoryKiB=%d, want it to differ from the weak digest's parameters", argonMemoryKiB)
	}

	ok, err := verifyPassword(password, weak)
	if err != nil {
		t.Fatalf("verifyPassword()=%v, want=%v", err, nil)
	}
	if !ok {
		t.Error("verifyPassword()=false, want a digest with older parameters to still verify")
	}
}
