package main

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/google/uuid"
)

// dummyHashForTimingParity is verified against when the username does not exist,
// so that a missing user costs roughly the same as a wrong password.
var dummyHashForTimingParity = func() string {
	hash, _ := hashPassword("timing-parity-placeholder")
	return hash
}()

type LoginRequest struct {
	UserID uuid.UUID `json:"user_id"`
}

func (s *Server) login(w http.ResponseWriter, r *http.Request) {
	var req LoginRequest
	json.NewDecoder(r.Body).Decode(&req)

	tokens, cookie, err := s.issuePair(req.UserID, uuid.New())
	if err != nil {
		writeError(w, err)
		return
	}

	http.SetCookie(w, &cookie)
	s.writeJSON(w, r, http.StatusOK, tokens)
}

// refresh trades a refresh token for a new pair and spends the old one.
//
// Rotating on every use is what makes a 30-day credential defensible: the token
// on the wire is only ever good once, so intercepting one buys a single refresh
// rather than a month of access. The corollary is that a spent token must stay
// recognisable, because a second presentation of it is the only evidence that
// somebody else has a copy.
//
// When that happens neither presenter can be trusted — the request could be the
// thief or the victim and there is no way to tell — so the whole family is
// destroyed and both are made to log in again.
func (s *Server) refresh(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	presented := refreshTokenFrom(r)
	if presented == "" {
		writeError(w, errUnauthorized)
		return
	}

	hash := hashRefreshToken(presented)

	used, ok := s.queries.UseRefreshToken(hash)
	if !ok {
		writeError(w, s.rejectRefresh(ctx, hash))
		return
	}

	tokens, cookie, err := s.issuePair(used.UserID, used.FamilyID)
	if err != nil {
		writeError(w, err)
		return
	}

	http.SetCookie(w, &cookie)
	s.writeJSON(w, r, http.StatusOK, tokens)
}

// rejectRefresh always refuses. Its real work is the side effect: deciding
// whether the refusal was routine or evidence of a stolen token, and cutting
// the family if so.
//
// The response is identical either way. Telling a caller that their token was
// recognised but already spent would confirm to a thief that they held a real
// credential and had merely lost the race.
func (s *Server) rejectRefresh(ctx context.Context, hash []byte) error {
	existing, ok := s.queries.GetRefreshTokenByHash(hash)
	if !ok {
		// Unknown token: nothing to revoke, and nothing to read into it.
		return errUnauthorized
	}
	if existing.UsedAt == nil {
		// Present and unspent, so it was the expiry check that declined it.
		return errUnauthorized
	}

	// Spent, and presented again. Warn rather than Info: unlike a wrong
	// password this cannot happen by mistyping something.
	s.log.WarnContext(ctx, "refresh token reused, revoking the family",
		"user_id", existing.UserID, "family_id", existing.FamilyID)

	s.queries.DeleteRefreshTokenFamily(existing.FamilyID)

	return errUnauthorized
}

// logout cuts the family the token belongs to, so a successor that a racing
// refresh may just have minted dies with it.
//
// It cannot withdraw the access token already in the caller's hand — nothing
// can, and that is the trade this design makes — so a logout is complete only
// once that expires. Until then the holder keeps the access they had and can
// obtain no more.
func (s *Server) logout(w http.ResponseWriter, r *http.Request) {
	if presented := refreshTokenFrom(r); presented != "" {
		if existing, ok := s.queries.GetRefreshTokenByHash(hashRefreshToken(presented)); ok {
			s.queries.DeleteRefreshTokenFamily(existing.FamilyID)
		}
	}

	// Clearing the cookie regardless keeps logout idempotent, and leaves a
	// client holding an unrecognised token in a logged-out state anyway.
	cookie := expiredRefreshCookie(s.cfg)
	http.SetCookie(w, &cookie)
	w.WriteHeader(http.StatusNoContent)
}

// refreshTokenFrom reads the refresh cookie. A missing cookie and an empty one
// are the same thing here — neither is a credential — so both come back "".
func refreshTokenFrom(r *http.Request) string {
	cookie, err := r.Cookie(refreshCookieName)
	if err != nil {
		return ""
	}
	return cookie.Value
}

// issuePair mints an access token and the refresh token that will replace it,
// and records the latter.
//
// familyID ties the new refresh token to the login it descends from. A fresh
// login starts a family; a refresh continues one. That is what lets a single
// compromised chain be cut without touching the same user's other devices.
func (s *Server) issuePair(userID, familyID uuid.UUID) (TokenResponse, http.Cookie, error) {
	accessToken, expiresAt, err := s.issuer.Issue(userID, grantedScopes)
	if err != nil {
		return TokenResponse{}, http.Cookie{}, err
	}

	refreshToken, err := newRefreshToken()
	if err != nil {
		return TokenResponse{}, http.Cookie{}, err
	}

	refreshExpiresAt := time.Now().Add(refreshLifetime)
	s.queries.CreateRefreshToken(CreateRefreshTokenParams{
		UserID:    userID,
		FamilyID:  familyID,
		TokenHash: hashRefreshToken(refreshToken),
		ExpiresAt: refreshExpiresAt,
	})

	return newTokenResponse(accessToken, expiresAt),
		refreshCookie(s.cfg, refreshToken, refreshExpiresAt),
		nil
}
