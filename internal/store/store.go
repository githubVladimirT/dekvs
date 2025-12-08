package store

import (
	"context"
	"time"

	"github.com/githubVladimirT/dekvs/pkg/types"
)

type Store interface {
	Get(ctx context.Context, key string) (*types.Response, error)
	Put(ctx context.Context, key string, value []byte, opts ...Option) error
	Delete(ctx context.Context, key string) error

	BeginTransaction() (Transaction, error)
	BeginTransactionWithContext(ctx context.Context) (Transaction, error)
	BeginTransactionWithOptions(ctx context.Context, timeout time.Duration, isolation IsolationLevel) (Transaction, error)

	Clear() error

	Close() error
}

type Snapshotter interface {
	Export() ([]byte, error)
	Import(data []byte) error
	Clear() error
	LastSnapshotTime() time.Time
}

type Transaction interface {
	Put(key string, value []byte, opts ...Option) error
	Delete(key string) error
	Commit() error
	Rollback() error
	GetState() TransactionState
	GetOperationsCount() int
}

type TransactionState int

const (
	TransactionActive TransactionState = iota
	TransactionCommitted
	TransactionRolledBack
	TransactionFailed
	TransactionTimeout
)

type IsolationLevel int

const (
	ReadCommitted IsolationLevel = iota
	RepeatableRead
	Serializable
)

type options struct {
	ttl     time.Duration
	version int64
	schema  any // interface{} --> any
}

type Option func(*options)

func WithTTL(ttl time.Duration) Option {
	return func(o *options) {
		o.ttl = ttl
	}
}

func WithVersion(version int64) Option {
	return func(o *options) {
		o.version = version
	}
}

func WithSchema(schema any) Option { // interface{} --> any
	return func(o *options) {
		o.schema = schema
	}
}
