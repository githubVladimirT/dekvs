package types

import "time"

// Operation types
const (
	OpPut    = "PUT"
	OpDelete = "DELETE"
	OpGet    = "GET"
)

// Command для Raft репликации
type Command struct {
	Op      string        `json:"op"`
	Key     string        `json:"key"`
	Value   []byte        `json:"value,omitempty"`
	TTL     time.Duration `json:"ttl,omitempty"`
	Version int64         `json:"version,omitempty"`
}

// Response от хранилища
type Response struct {
	Value   []byte `json:"value,omitempty"`
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
	Version int64  `json:"version,omitempty"`
}
