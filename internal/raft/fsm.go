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

type FSM struct {
	store store.Store
	mu    sync.Mutex
}

func NewFSM(store store.Store) *FSM {
	return &FSM{
		store: store,
	}
}

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

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return &simpleSnapshot{}, nil
}

func (f *FSM) Restore(rc io.ReadCloser) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	return rc.Close()
}

type simpleSnapshot struct{}

func (s *simpleSnapshot) Persist(sink raft.SnapshotSink) error {
	_, err := sink.Write([]byte("{}"))
	if err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *simpleSnapshot) Release() {
	return
}
