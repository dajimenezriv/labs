package main

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"golang.org/x/crypto/argon2"
)

// Argon2id parameters, following the OWASP Password Storage Cheat Sheet's
// baseline: 19 MiB of memory, 2 iterations, 1 degree of parallelism.
//
// Raising these strengthens hashes; lowering them weakens hashes created from
// then on. Existing hashes keep working either way, because the parameters used
// are recorded in each encoded hash rather than read from these constants at
// verification time.
// Argon doesn't cap the password length.
const (
	argonMemoryKiB  = 19 * 1024
	argonIterations = 2
	argonThreads    = 1
	argonSaltLength = 16
	argonKeyLength  = 32
)

var (
	errInvalidHash        = errors.New("password hash is not in PHC format")
	errIncompatibleArgon2 = errors.New("password hash uses an incompatible argon2 version")
)

// hashPassword returns an Argon2id digest in PHC string format:
//
//	$argon2id$v=19$m=19456,t=2,p=1$<base64 salt>$<base64 key>
//
// The salt and parameters travel with the digest, so verification never depends
// on the constants above still holding the same values.
func hashPassword(password string) (string, error) {
	salt := make([]byte, argonSaltLength)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("generating salt: %w", err)
	}

	key := argon2.IDKey([]byte(password), salt, argonIterations, argonMemoryKiB, argonThreads, argonKeyLength)

	return fmt.Sprintf("$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s",
		argon2.Version,
		argonMemoryKiB, argonIterations, argonThreads,
		base64.RawStdEncoding.EncodeToString(salt),
		base64.RawStdEncoding.EncodeToString(key),
	), nil
}

// verifyPassword reports whether password matches the given encoded digest.
// A malformed digest is an error, not simply a mismatch, so callers can tell
// "wrong password" apart from "corrupt row".
func verifyPassword(password, encoded string) (bool, error) {
	// A well-formed digest splits into 6 parts, the first being empty because
	// the string starts with '$'.
	parts := strings.Split(encoded, "$")
	if len(parts) != 6 || parts[0] != "" || parts[1] != "argon2id" {
		return false, errInvalidHash
	}

	var version int
	if _, err := fmt.Sscanf(parts[2], "v=%d", &version); err != nil {
		return false, errInvalidHash
	}
	if version != argon2.Version {
		return false, errIncompatibleArgon2
	}

	var memory, iterations uint32
	var threads uint8
	if _, err := fmt.Sscanf(parts[3], "m=%d,t=%d,p=%d", &memory, &iterations, &threads); err != nil {
		return false, errInvalidHash
	}

	salt, err := base64.RawStdEncoding.DecodeString(parts[4])
	if err != nil {
		return false, errInvalidHash
	}
	key, err := base64.RawStdEncoding.DecodeString(parts[5])
	if err != nil {
		return false, errInvalidHash
	}
	if len(salt) == 0 || len(key) == 0 {
		return false, errInvalidHash
	}

	// Recompute with the parameters recorded in the digest, not the current
	// constants, so old hashes still verify after a parameter change.
	candidate := argon2.IDKey([]byte(password), salt, iterations, memory, threads, uint32(len(key)))

	// Constant-time: a length-dependent or early-exit comparison would leak how
	// much of the digest an attacker guessed correctly.
	return subtle.ConstantTimeCompare(key, candidate) == 1, nil
}
