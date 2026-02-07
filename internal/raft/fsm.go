package raft

import "github.com/githubVladimirT/dekvs/internal/store"

type FSMManager interface {
	Get(key string) ([]byte, uint64, bool)
	GetVersionHistory(key string, limit int) []*store.VersionedValue
	CleanupExpired() int
}
