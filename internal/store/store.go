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
	Close() error
}

type Transaction interface {
	Put(key string, value []byte, opts ...Option) error
	Delete(key string) error
	Commit() error
	Rollback() error
}

type options struct {
	ttl     time.Duration
	version int64
	schema  interface{}
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

func WithSchema(schema interface{}) Option {
	return func(o *options) {
		o.schema = schema
	}
}
