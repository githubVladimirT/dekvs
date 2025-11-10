package raft

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/githubVladimirT/dekvs/internal/store"
	"github.com/githubVladimirT/dekvs/pkg/types"

	"github.com/hashicorp/raft"
)

// FSM implements the raft.FSM interface
type FSM struct {
	store store.Store
	mu    sync.Mutex
}

// NewFSM creates a new FSM
func NewFSM(store store.Store) *FSM {
	return &FSM{
		store: store,
	}
}

// Apply applies a Raft log entry to the FSM
func (f *FSM) Apply(log *raft.Log) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()

	var cmd types.Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		return fmt.Errorf("failed to unmarshal command: %v", err)
	}

	ctx := context.Background()
	switch cmd.Op {
	case types.OpPut:
		var opts []store.Option
		if cmd.TTL > 0 {
			opts = append(opts, store.WithTTL(cmd.TTL))
		}
		if err := f.store.Put(ctx, cmd.Key, cmd.Value, opts...); err != nil {
			return err
		}
		return nil

	case types.OpDelete:
		if err := f.store.Delete(ctx, cmd.Key); err != nil {
			return err
		}
		return nil

	default:
		return fmt.Errorf("unknown operation: %s", cmd.Op)
	}
}

// TODO: Add snapshots implementation!!

// Snapshot returns a snapshot of the FSM
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// For now, return a simple snapshot
	return &simpleSnapshot{}, nil
}

// Restore restores an FSM from a snapshot
func (f *FSM) Restore(rc io.ReadCloser) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// For now, we'll just close the reader
	return rc.Close()
}

// simpleSnapshot is a basic snapshot implementation
type simpleSnapshot struct{}

func (s *simpleSnapshot) Persist(sink raft.SnapshotSink) error {
	// Write empty snapshot for now
	_, err := sink.Write([]byte("{}"))
	if err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *simpleSnapshot) Release() {
	// No resources to release
}
