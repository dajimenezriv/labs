package main

import (
	"context"
	"net/http"
	"strings"

	"github.com/google/uuid"
)

// Principal is who a request is from. Only the id: this service holds the
// users table, so anything else about the caller is a lookup away rather than
// something to carry around.
type Principal struct {
	UserID uuid.UUID
}

type contextKey struct{ name string }

var principalContextKey = &contextKey{"principal"}

func principalFromContext(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(principalContextKey).(Principal)
	return p, ok
}

// requireAuth rejects the request unless it carries a token this service can
// verify, and passes the caller down through the request context.
func (s *Server) requireAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		scheme, raw, ok := strings.Cut(r.Header.Get("Authorization"), " ")
		if !ok || !strings.EqualFold(scheme, "bearer") || raw == "" {
			s.fail(w, r, errUnauthorized)
			return
		}

		claims, err := s.verifier.Verify(ctx, raw)
		if err != nil {
			// Info, not Error: an expired or malformed token is a client
			// problem and a routine one. At error level every token that aged
			// out would look like a fault of ours.
			//
			// One case hiding in here is worth knowing about: if identity has
			// rotated its key and this process cannot reach the JWKS endpoint,
			// the failure arrives as an unresolvable kid and looks from the
			// outside like a bad token. The logged error is what tells the two
			// apart.
			s.log.InfoContext(ctx, "verify token", "err", err)
			s.fail(w, r, errUnauthorized)
			return
		}

		userID, err := claims.UserID()
		if err != nil {
			s.log.InfoContext(ctx, "read subject", "err", err)
			s.fail(w, r, errUnauthorized)
			return
		}

		next.ServeHTTP(w, r.WithContext(context.WithValue(ctx, principalContextKey, Principal{UserID: userID})))
	})
}
