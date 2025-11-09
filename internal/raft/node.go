package raft

import (
	"context"
	"dekvs/internal/store"
	"dekvs/pkg/types" // Ensure this import exists
	"encoding/json"
	"time"
)

// Config для Raft ноды
type Config struct {
	NodeID   string
	RaftAddr string
	DataDir  string
}

type Node struct {
	id       string
	addr     string
	store    store.Store
	isReady  bool
	isLeader bool
}

func NewNode(config *Config, store store.Store) (*Node, error) {
	node := &Node{
		id:    config.NodeID,
		addr:  config.RaftAddr,
		store: store,
	}

	// Initialize Raft consensus (заглушка)
	node.isReady = true
	node.isLeader = true // временно делаем лидером для тестирования

	return node, nil
}

func (n *Node) IsLeader() bool {
	return n.isLeader
}

func (n *Node) SetLeader(isLeader bool) {
	n.isLeader = isLeader
}

func (n *Node) ApplyCommand(cmd types.Command) error { // Use types.Command here
	if !n.IsLeader() {
		return types.ErrNotLeader
	}

	// Replicate command through Raft
	data, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	return n.applyRaftLog(data)
}

func (n *Node) applyRaftLog(data []byte) error {
	var cmd types.Command // Use types.Command here
	if err := json.Unmarshal(data, &cmd); err != nil {
		return err
	}

	ctx := context.Background()
	switch cmd.Op {
	case types.OpPut:
		// Convert TTL from int64 to time.Duration for the store
		var ttlOption store.Option
		if cmd.TTL > 0 {
			ttlOption = store.WithTTL(time.Duration(cmd.TTL))
		} else {
			ttlOption = store.WithTTL(0)
		}
		return n.store.Put(ctx, cmd.Key, cmd.Value, ttlOption)
	case types.OpDelete:
		return n.store.Delete(ctx, cmd.Key)
	default:
		return types.ErrInvalidOperation
	}
}

func (n *Node) Get(key string) (*types.Response, error) {
	ctx := context.Background()
	return n.store.Get(ctx, key)
}
