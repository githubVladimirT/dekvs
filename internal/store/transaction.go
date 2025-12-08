package store

import (
	"context"
	"fmt"
	"sync"

	"time"

	"github.com/githubVladimirT/dekvs/pkg/types"
)

type memoryTransaction struct {
	store          *memoryStore
	ops            []operation
	backup         *storeBackup
	state          TransactionState
	ctx            context.Context
	cancel         context.CancelFunc
	timeout        time.Duration
	isolationLevel IsolationLevel
	startTime      time.Time
	mu             sync.RWMutex
}

type storeBackup struct {
	data    map[string]*item
	version int64
	time    time.Time
}

type operation struct {
	op    string
	key   string
	value []byte
	opts  []Option
}

func (tx *memoryTransaction) Put(key string, value []byte, opts ...Option) error {
	if err := tx.checkContext(); err != nil {
		return err
	}

	tx.mu.Lock()
	defer tx.mu.Unlock()

	tx.ops = append(tx.ops, operation{
		op:    "PUT",
		key:   key,
		value: value,
		opts:  opts,
	})

	fmt.Printf("Transaction: Queued PUT for key: %s\n", key)
	return nil
}

func (tx *memoryTransaction) Delete(key string) error {
	if err := tx.checkContext(); err != nil {
		return err
	}

	tx.mu.Lock()
	defer tx.mu.Unlock()

	tx.ops = append(tx.ops, operation{
		op:  "DELETE",
		key: key,
	})

	fmt.Printf("Transaction: Queued DELETE for key: %s\n", key)
	return nil
}

func (tx *memoryTransaction) validateTransaction() error {
	tempData := make(map[string]*item)
	tempVersion := tx.store.version

	tx.store.mu.RLock()
	for k, v := range tx.store.data {
		tempData[k] = v
	}
	tx.store.mu.RUnlock()

	for _, op := range tx.ops {
		switch op.op {
		case "PUT":
			options := &options{}
			for _, opt := range op.opts {
				opt(options)
			}

			if options.version > 0 {
				if existing, exists := tempData[op.key]; exists && existing.Version != options.version {
					return types.ErrVersionConflict
				}
			}

			if options.ttl > 0 {
				expiry := time.Now().Add(options.ttl)
				if expiry.Before(time.Now()) {
					return fmt.Errorf("TTL would expire immediately for key: %s", op.key)
				}
			}

			tempVersion++
			tempData[op.key] = &item{
				Value:   op.value,
				Version: tempVersion,
			}
			if options.ttl > 0 {
				expiry := time.Now().Add(options.ttl)
				tempData[op.key].Expiry = &expiry
			}

		case "DELETE":
			delete(tempData, op.key)
		}
	}

	return nil
}

func (tx *memoryTransaction) Commit() error {
	tx.store.mu.Lock()
	defer tx.store.mu.Unlock()

	fmt.Printf("MemoryStore.Transaction: Committing %d operations\n", len(tx.ops))

	if tx.state != TransactionActive {
		return fmt.Errorf("transaction is not active (state: %v)", tx.state)
	}

	if err := tx.checkContext(); err != nil {
		return err
	}

	fmt.Printf("MemoryStore.Transaction: Committing %d operations\n", len(tx.ops))

	if err := tx.validateTransaction(); err != nil {
		tx.state = TransactionFailed
		fmt.Printf("Transaction validation failed: %v\n", err)
		return err
	}

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
				if existing, exists := tx.store.data[op.key]; exists && existing.Version != options.version {
					tx.state = TransactionFailed
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
			fmt.Printf("Transaction: Applied PUT for key: %s\n", op.key)

		case "DELETE":
			delete(tx.store.data, op.key)
			fmt.Printf("Transaction: Applied DELETE for key: %s\n", op.key)
		}
	}

	tx.state = TransactionCommitted
	if tx.cancel != nil {
		tx.cancel()
	}

	fmt.Printf("MemoryStore.Transaction: Commit successful, total keys: %d\n", len(tx.store.data))
	return nil

}

func (tx *memoryTransaction) Rollback() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.state != TransactionActive {
		return fmt.Errorf("transaction is not active (state: %v)", tx.state)
	}

	fmt.Printf("MemoryStore.Transaction: Rolling back %d operations\n", len(tx.ops))

	if tx.backup != nil {
		tx.store.restoreBackup(tx.backup)
	}

	tx.state = TransactionRolledBack
	if tx.cancel != nil {
		tx.cancel()
	}

	fmt.Println("MemoryStore.Transaction: Rollback successful")

	return nil
}

func (tx *memoryTransaction) GetState() TransactionState {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	return tx.state
}

func (tx *memoryTransaction) GetOperationsCount() int {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	return len(tx.ops)
}

func (m *memoryStore) BeginTransaction() (Transaction, error) {
	return m.BeginTransactionWithContext(context.Background())
}

func (m *memoryStore) BeginTransactionWithContext(ctx context.Context) (Transaction, error) {
	return m.BeginTransactionWithOptions(ctx, 30*time.Second, ReadCommitted)
}

func (m *memoryStore) BeginTransactionWithOptions(ctx context.Context, timeout time.Duration, isolation IsolationLevel) (Transaction, error) {
	fmt.Printf("MemoryStore: Beginning transaction with timeout %v and isolation %v\n", timeout, isolation)

	if ctx == nil {
		ctx = context.Background()
	}

	var cancel context.CancelFunc
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, timeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}

	backup := m.createBackup()

	tx := &memoryTransaction{
		store:          m,
		ops:            make([]operation, 0),
		backup:         backup,
		state:          TransactionActive,
		ctx:            ctx,
		cancel:         cancel,
		timeout:        timeout,
		isolationLevel: isolation,
		startTime:      time.Now(),
	}

	return tx, nil
}

func (m *memoryStore) createBackup() *storeBackup {
	m.mu.RLock()
	defer m.mu.RUnlock()

	backup := &storeBackup{
		data:    make(map[string]*item),
		version: m.version,
		time:    time.Now(),
	}

	for k, v := range m.data {
		itemCopy := &item{
			Value:   make([]byte, len(v.Value)),
			Version: v.Version,
		}
		copy(itemCopy.Value, v.Value)

		if v.Expiry != nil {
			expiryCopy := *v.Expiry
			itemCopy.Expiry = &expiryCopy
		}

		backup.data[k] = itemCopy
	}

	return backup
}

func (m *memoryStore) restoreBackup(backup *storeBackup) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.data = make(map[string]*item)
	for k, v := range backup.data {
		m.data[k] = v
	}
	m.version = backup.version

	fmt.Printf("MemoryStore: Restored backup from %v (version: %d, keys: %d)\n",
		backup.time.Format(time.RFC3339), backup.version, len(backup.data))
}

func (tx *memoryTransaction) checkContext() error {
	tx.mu.RLock()
	state := tx.state
	defer tx.mu.RUnlock()

	if state != TransactionActive {
		return fmt.Errorf("transaction is not active (state: %v)", state)
	}

	select {
	case <-tx.ctx.Done():
		tx.mu.Lock()
		if tx.state == TransactionActive {
			tx.state = TransactionTimeout
		}
		tx.mu.Unlock()
		return fmt.Errorf("transaction canceled: %v", tx.ctx.Err())
	default:
		return nil
	}
}
