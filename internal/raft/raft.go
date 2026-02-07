package raft

import (
	"encoding/json"
	"io"
	"time"

	"github.com/githubVladimirT/dekvs/internal/store"
	"github.com/hashicorp/raft"
)

type Command struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value []byte `json:"value"`
	TTL   int64  `json:"ttl"`
}

type BatchCommand struct {
	Ops []Command `json:"ops"`
}

type FSM struct {
	store   *store.Store
	manager FSMManager
}

func NewFSM(cacheSize int) *FSM {
	fsm := &FSM{
		store: store.NewStore(cacheSize),
	}
	fsm.manager = fsm
	return fsm
}

func (f *FSM) Apply(log *raft.Log) any {
	var cmd Command
	var batch BatchCommand

	if err := json.Unmarshal(log.Data, &batch); err == nil && len(batch.Ops) > 0 {
		results := make([]any, len(batch.Ops))

		for i, op := range batch.Ops {
			results[i] = f.applyCommand(op)
		}

		return results
	}

	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		return []any{nil}
	}

	return []any{f.applyCommand(cmd)}
}

func (f *FSM) applyCommand(cmd Command) any {
	switch cmd.Op {
	case "set":
		var ttl time.Duration

		if cmd.TTL > 0 {
			ttl = time.Duration(cmd.TTL) * time.Second
		}

		version := f.store.Set(cmd.Key, cmd.Value, ttl)
		return version
	case "delete":
		success := f.store.Delete(cmd.Key)
		return success
	}

	return nil
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	return &snapshot{
		Data:    f.store.Snapshot(),
		Version: f.store.GetVersion(),
	}, nil
}

func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	var data []byte

	if _, err := rc.Read(data); err != nil {
		return err
	}

	var snap snapshot

	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	f.store.Restore(snap.Data, snap.Version)
	return nil
}

func (f *FSM) Get(key string) ([]byte, uint64, bool) {
	val, exists := f.store.Get(key)
	if !exists {
		return nil, 0, false
	}
	return val.Value, val.Version, true
}

func (f *FSM) GetVersionHistory(key string, limit int) []*store.VersionedValue {
	return f.store.GetVersionHistory(key, limit)
}

func (f *FSM) CleanupExpired() int {
	return f.store.CleanupExpired()
}
