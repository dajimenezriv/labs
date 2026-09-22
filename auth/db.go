package main

import (
	"maps"
	"sync"
	"time"

	"github.com/google/uuid"
)

type Queries struct {
	mu            sync.Mutex
	refreshTokens map[string]RefreshToken
}

func NewQueries() *Queries {
	return &Queries{refreshTokens: map[string]RefreshToken{}}
}

type RefreshToken struct {
	ID        uuid.UUID
	UserID    uuid.UUID
	FamilyID  uuid.UUID
	TokenHash []byte
	ExpiresAt time.Time
	UsedAt    *time.Time
	CreatedAt time.Time
}

type CreateRefreshTokenParams struct {
	UserID    uuid.UUID
	FamilyID  uuid.UUID
	TokenHash []byte
	ExpiresAt time.Time
}

func (q *Queries) CreateRefreshToken(arg CreateRefreshTokenParams) {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.refreshTokens[string(arg.TokenHash)] = RefreshToken{
		ID:        uuid.New(),
		UserID:    arg.UserID,
		FamilyID:  arg.FamilyID,
		TokenHash: arg.TokenHash,
		ExpiresAt: arg.ExpiresAt,
		CreatedAt: time.Now(),
	}
}

func (q *Queries) UseRefreshToken(tokenHash []byte) (RefreshToken, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	rt, ok := q.refreshTokens[string(tokenHash)]
	if !ok || rt.UsedAt != nil || !rt.ExpiresAt.After(time.Now()) {
		return RefreshToken{}, false
	}

	now := time.Now()
	rt.UsedAt = &now
	q.refreshTokens[string(tokenHash)] = rt

	return rt, true
}

func (q *Queries) GetRefreshTokenByHash(tokenHash []byte) (RefreshToken, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	rt, ok := q.refreshTokens[string(tokenHash)]
	return rt, ok
}

func (q *Queries) DeleteRefreshTokenFamily(familyID uuid.UUID) {
	q.mu.Lock()
	defer q.mu.Unlock()

	maps.DeleteFunc(q.refreshTokens, func(_ string, rt RefreshToken) bool {
		return rt.FamilyID == familyID
	})
}
