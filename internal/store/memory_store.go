package store

import (
	"context"
	"dekvs/pkg/types"
	"sync"
	"time"
)

type memoryStore struct {
	mu      sync.RWMutex
	data    map[string]*item
	version int64
}

type item struct {
	value   []byte
	version int64
	expiry  *time.Time
}

func NewMemoryStore() Store {
	return &memoryStore{
		data: make(map[string]*item),
	}
}

func (m *memoryStore) Get(ctx context.Context, key string) (*types.Response, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	item, exists := m.data[key]
	if !exists {
		return nil, types.ErrKeyNotFound
	}

	if item.expiry != nil && time.Now().After(*item.expiry) {
		delete(m.data, key)
		return nil, types.ErrKeyNotFound
	}

	return &types.Response{
		Value:   item.value,
		Success: true,
		Version: item.version,
	}, nil
}

func (m *memoryStore) Put(ctx context.Context, key string, value []byte, opts ...Option) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	options := &options{}
	for _, opt := range opts {
		opt(options)
	}

	// Version check
	if options.version > 0 {
		if existing, exists := m.data[key]; exists && existing.version != options.version {
			return types.ErrVersionConflict
		}
	}

	m.version++
	item := &item{
		value:   value,
		version: m.version,
	}

	if options.ttl > 0 {
		expiry := time.Now().Add(options.ttl)
		item.expiry = &expiry
	}

	m.data[key] = item
	return nil
}

func (m *memoryStore) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.data[key]; !exists {
		return types.ErrKeyNotFound
	}

	delete(m.data, key)
	return nil
}

func (m *memoryStore) BeginTransaction() (Transaction, error) {
	return &memoryTransaction{
		store: m,
		ops:   make([]operation, 0),
	}, nil
}

func (m *memoryStore) Close() error {
	m.data = make(map[string]*item)
	return nil
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

	for _, op := range tx.ops {
		switch op.op {
		case "PUT":
			options := &options{}
			for _, opt := range op.opts {
				opt(options)
			}

			if options.version > 0 {
				if existing, exists := tx.store.data[op.key]; exists && existing.version != options.version {
					return types.ErrVersionConflict
				}
			}

			tx.store.version++
			item := &item{
				value:   op.value,
				version: tx.store.version,
			}

			if options.ttl > 0 {
				expiry := time.Now().Add(options.ttl)
				item.expiry = &expiry
			}

			tx.store.data[op.key] = item

		case "DELETE":
			delete(tx.store.data, op.key)
		}
	}
	return nil
}

func (tx *memoryTransaction) Rollback() error {
	tx.ops = make([]operation, 0)
	return nil
}

