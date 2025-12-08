package store

import (
	"context"
	"encoding/json"
	"fmt"

	"sync"
	"time"

	"github.com/githubVladimirT/dekvs/pkg/types"
)

type memoryStore struct {
	mu               sync.RWMutex
	data             map[string]*item
	version          int64
	lastSnapshotTime time.Time
}

type item struct {
	Value   []byte     `json:"value"`
	Version int64      `json:"version"`
	Expiry  *time.Time `json:"expiry,omitempty"`
}

type SnapshotData struct {
	Version   int64             `json:"version"`
	Timestamp time.Time         `json:"timestamp"`
	KeyCount  int               `json:"key_count"`
	Data      map[string]*item  `json:"data"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

var _ Store = (*memoryStore)(nil)
var _ Snapshotter = (*memoryStore)(nil)

func NewMemoryStore() Store {
	fmt.Println("MemoryStore: Creating new memory store")
	return &memoryStore{
		data:             make(map[string]*item),
		lastSnapshotTime: time.Now(),
	}
}

func (m *memoryStore) Get(ctx context.Context, key string) (*types.Response, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	fmt.Printf("MemoryStore.Get: Looking for key: %s\n", key)

	item, exists := m.data[key]
	if !exists {
		fmt.Printf("MemoryStore.Get: Key not found: %s\n", key)
		return nil, types.ErrKeyNotFound
	}

	if item.Expiry != nil && time.Now().After(*item.Expiry) {
		fmt.Printf("MemoryStore.Get: Key expired: %s\n", key)
		return nil, types.ErrKeyNotFound
	}

	fmt.Printf("MemoryStore.Get: Found key: %s, value length: %d\n", key, len(item.Value))

	return &types.Response{
		Value:   item.Value,
		Success: true,
		Version: item.Version,
	}, nil
}

func (m *memoryStore) Put(ctx context.Context, key string, value []byte, opts ...Option) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	fmt.Printf("MemoryStore.Put: Storing key: %s, value length: %d\n", key, len(value))

	options := &options{}
	for _, opt := range opts {
		opt(options)
	}

	if options.version > 0 {
		if existing, exists := m.data[key]; exists && existing.Version != options.version {
			fmt.Printf("MemoryStore.Put: Version conflict for key: %s\n", key)
			return types.ErrVersionConflict
		}
	}

	m.version++
	item := &item{
		Value:   value,
		Version: m.version,
	}

	if options.ttl > 0 {
		expiry := time.Now().Add(options.ttl)
		item.Expiry = &expiry
		fmt.Printf("MemoryStore.Put: Setting TTL for key: %s, expiry: %v\n", key, expiry)

	}

	m.data[key] = item
	fmt.Printf("MemoryStore.Put: Successfully stored key: %s, total keys: %d\n", key, len(m.data))

	return nil
}

func (m *memoryStore) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	fmt.Printf("MemoryStore.Delete: Deleting key: %s\n", key)

	delete(m.data, key)

	return nil
}

func (m *memoryStore) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = make(map[string]*item)
	return nil
}

func (m *memoryStore) Export() ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	fmt.Printf("MemoryStore.Export: Exporting %d keys\n", len(m.data))

	snapshot := SnapshotData{
		Version:   m.version,
		Timestamp: time.Now(),
		KeyCount:  len(m.data),
		Data:      make(map[string]*item),
		Metadata: map[string]string{
			"exported_by": "dekvs",
			"format":      "v2",
		},
	}

	now := time.Now()
	validKeys := 0
	for key, it := range m.data {
		if it.Expiry != nil && now.After(*it.Expiry) {
			continue
		}
		snapshot.Data[key] = &item{
			Value:   append([]byte{}, it.Value...),
			Version: it.Version,
			Expiry:  it.Expiry,
		}
		validKeys++
	}

	snapshot.KeyCount = validKeys
	fmt.Printf("MemoryStore.Export: Exported %d valid keys (skipped %d expired)\n",
		validKeys, len(m.data)-validKeys)

	data, err := json.Marshal(snapshot)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal snapshot: %v", err)
	}

	return data, nil
}

func (m *memoryStore) Import(data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	fmt.Printf("MemoryStore.Import: Importing %d bytes\n", len(data))

	var snapshot SnapshotData
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return fmt.Errorf("failed to unmarshal snapshot: %v", err)
	}

	if snapshot.Data == nil {
		return fmt.Errorf("invalid snapshot: data is nil")
	}

	m.data = snapshot.Data
	m.version = snapshot.Version

	fmt.Printf("MemoryStore.Import: Successfully imported %d keys, version: %d (snapshot from %s)\n",
		snapshot.KeyCount, snapshot.Version, snapshot.Timestamp.Format(time.RFC3339))

	// fmt.Printf("[!] MemoryStore.Import: DATA: %s", m.data)

	return nil
}

func (m *memoryStore) Clear() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	fmt.Println("MemoryStore.Clear: Clearing all data")
	m.data = make(map[string]*item)
	m.version = 0

	return nil
}

func (m *memoryStore) LastSnapshotTime() time.Time {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.lastSnapshotTime
}
