package store

import (
	"context"
	"encoding/json"
	"fmt"

	// "errors"
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

func (m *memoryStore) BeginTransaction() (Transaction, error) {
	fmt.Println("MemoryStore: Beginning transaction")

	return &memoryTransaction{
		store: m,
		ops:   make([]operation, 0),
	}, nil
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

	dataCopy := make(map[string]*item)
	for k, v := range m.data {
		if v.Expiry != nil && time.Now().After(*v.Expiry) {
			continue
		}

		dataCopy[k] = &item{
			Value:   v.Value,
			Version: v.Version,
			Expiry:  v.Expiry,
		}
	}

	snapshot := struct {
		Data      map[string]*item `json:"data"`
		Version   int64            `json:"version"`
		Timestamp time.Time        `json:"timestamp"`
	}{
		Data:      m.data,
		Version:   m.version,
		Timestamp: time.Now(),
	}

	data, err := json.Marshal(snapshot)
	if err != nil {
		fmt.Printf("MemoryStore.Export: Marshal failed: %v\n", err)
		return nil, err
	}

	fmt.Printf("MemoryStore.Export: Successfully exported %d bytes (%d keys)\n", len(data), len(dataCopy))
	return data, nil
}

func (m *memoryStore) Import(data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	fmt.Printf("MemoryStore.Import: Importing %d bytes\n", len(data))

	var snapshot struct {
		Data      map[string]*item `json:"data"`
		Version   int64            `json:"version"`
		Timestamp time.Time        `json:"timestamp"`
	}

	if err := json.Unmarshal(data, &snapshot); err != nil {
		fmt.Printf("MemoryStore.Import: Unmarshal failed: %v\n", err)
		return err
	}

	m.data = snapshot.Data
	m.version = snapshot.Version
	m.lastSnapshotTime = snapshot.Timestamp

	fmt.Printf("MemoryStore.Import: Successfully imported %d keys, version: %d\n", len(m.data), m.version)
	return nil
}

func (m *memoryStore) LastSnapshotTime() time.Time {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.lastSnapshotTime
}

// Transaction implementation
type operation struct {
	op    string
	key   string
	value []byte
	opts  []Option
}

type memoryTransaction struct {
	store *memoryStore
	ops   []operation
}

func (tx *memoryTransaction) Put(key string, value []byte, opts ...Option) error {
	tx.ops = append(tx.ops, operation{
		op:    "PUT",
		key:   key,
		value: value,
		opts:  opts,
	})
	return nil
}

func (tx *memoryTransaction) Delete(key string) error {
	tx.ops = append(tx.ops, operation{
		op:  "DELETE",
		key: key,
	})
	return nil
}

func (tx *memoryTransaction) Commit() error {
	tx.store.mu.Lock()
	defer tx.store.mu.Unlock()

	fmt.Printf("MemoryStore.Transaction: Committing %d operations\n", len(tx.ops))

	for _, op := range tx.ops {
		switch op.op {
		case "PUT":
			options := &options{}
			for _, opt := range op.opts {
				opt(options)
			}

			if options.version > 0 {
				if existing, exists := tx.store.data[op.key]; exists && existing.Version != options.version {
					return types.ErrVersionConflict
				}
			}

			tx.store.version++
			item := &item{
				Value:   op.value,
				Version: tx.store.version,
			}

			if options.ttl > 0 {
				expiry := time.Now().Add(options.ttl)
				item.Expiry = &expiry
			}

			tx.store.data[op.key] = item

		case "DELETE":
			delete(tx.store.data, op.key)
		}
	}

	fmt.Println("MemoryStore.Transaction: Commit successful")

	return nil
}

func (tx *memoryTransaction) Rollback() error {
	fmt.Println("MemoryStore.Transaction: Rolling back transaction")

	tx.ops = make([]operation, 0)
	return nil
}
