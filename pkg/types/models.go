package types

import time

// Operation types
const (
	OpPut    = "PUT"
	OpDelete = "DELETE"
	OpGet    = "GET"
)

// Command for Raft replication
type Command struct {
    Op      string    `json:"op"`
    Key     string    `json:"key"`
    Value   []byte    `json:"value,omitempty"`
    TTL     time.Duration `json:"ttl,omitempty"`
    Version int64     `json:"version,omitempty"`
}

// Response from Vault
type Response struct {
	Value   []byte    `json:"value,omitempty"`
    Success bool      `json:"success"`
    Error   string    `json:"error,omitempty"`
    Version int64     `json:"version,omitempty"`
}

// NodeCache node configuration
type NodeConfig struct {
    ID        string   `yaml:"id"`
    Addr      string   `yaml:"addr"`
    RaftAddr  string   `yaml:"raft_addr"`
    Peers     []string `yaml:"peers"`
    DataDir   string   `yaml:"data_dir"`
}