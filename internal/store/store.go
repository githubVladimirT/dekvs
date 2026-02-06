package store

import (
	"sync"
	"time"
)

type VersionedValue struct {
	Value     []byte
	Version   uint64
	Timestamp time.Time
	ExpiresAt time.Time
}

type Store struct {
	mu       sync.RWMutex
	data     map[string]*VersionedValue
	versions map[string][]uint64
	version  uint64
	cache    Cache
}

func NewStore(cacheSize int) *Store {
	return &Store{
		data:     make(map[string]*VersionedValue),
		versions: make(map[string][]uint64),
		cache:    NewLRUCache(cacheSize),
	}
}

func (st *Store) Get(key string) (*VersionedValue, bool) {
	if cached, ok := st.cache.Get(key); ok {
		return cached.(*VersionValue), true
	}

	st.mu.RLock()
	defer st.mu.RUnlock()

	val, exists := st.data[key]
	if !exists {
		return nil, false
	}

	if !val.ExpiresAt.IsZero() && time.Now().After(val.ExpiresAt) {
		return nil, false
	}

	st.cache.Put(key, val)

	return val, true
}

func (st *Store) Set(key string, value []byte, ttl time.Duration) uint64 {
	st.mu.RLock()
	defer st.mu.RUnlock()

	st.version++
	expiresAt := time.Time{}
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl)
	}

	val := &VersionedValue{
		Value:     value,
		Version:   st.version,
		Timestamp: time.Now(),
		ExpiresAt: expiresAt,
	}

	st.data[key] = val
	st.versions[key] = append(st.versions[key], st.version)

	st.cache.Delete(key)

	return st.version
}

func (st *Store) Delete(key string) bool {
	st.mu.Lock()
	defer st.mu.Unlock()

	_, exists := st.data[key]
	if exists {
		delete(st.data, key)
		delete(st.versions, key)
		st.cache.Delete(key)

		return true
	}

	return false
}

func (st *Store) GetVersionHistory(key string, limit int) []*VersionedValue {
	st.mu.RLock()
	defer st.mu.RUnlock()

	versionIDs := st.versions[key]
	if len(versionIDs) == 0 {
		return nil
	}

	// TODO: hold here all versions
	// or get them from snapshots
	result := make([]*VersionedValue, 0, min(len(versionIDs), limit))

	for i := len(versionIDs) - 1; i >= 0 && len(result) < limit; i-- {
		// upload versions (reduced realization)
		result = append(result, st.data[key])
	}

	return result
}

func (st *Store) CleanupExpired() int {
	st.mu.Lock()
	defer st.mu.Unlock()

	cnt := 0
	now := time.Now()

	for key, val := range st.data {
		if !val.ExpiresAt.IsZero() && now.After(val.ExpiresAt) {
			delete(st.data, key)
			delete(st.versions, key)
			st.cache.Delete(key)
			cnt++
		}
	}

	return cnt
}

func (st *Store) Snapshot() map[string]*VersionedValue {
	st.mu.RLock()
	defer st.mu.RUnlock()

	snapshot := make(map[string]*VersionedValue)
	for k, v := range st.data {
		valCopy := *v
		valCopy.Value = make([]byte, len(v.Value))
		copy(valCopy.Value, v.Value)
		snapshot[k] = &valCopy
	}

	return snapshot
}

func (s *Store) Restore(snapshot map[string]*VersionedValue, version uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data = snapshot
	s.version = version
	s.cache.Clear()

	s.versions = make(map[string][]uint64)

	for key, val := range snapshot {
		s.versions[key] = []uint64{val.Version}
	}
}
