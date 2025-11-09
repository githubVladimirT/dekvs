package types

import "errors"

var (
	ErrKeyNotFound      = errors.New("key not found")
	ErrNotLeader        = errors.New("node is not leader")
	ErrTimeout          = errors.New("operation timeout")
	ErrVersionConflict  = errors.New("version conflict")
	ErrInvalidOperation = errors.New("invalid operation")
)
