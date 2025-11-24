package raft

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

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

	fmt.Printf("FSM.Apply: Applying log entry at index %d\n", log.Index)

	var cmd types.Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		return fmt.Errorf("failed to unmarshal command: %v", err)
	}

	fmt.Printf("FSM.Apply: Operation=%s, Key=%s, ValueLength=%d\n", cmd.Op, cmd.Key, len(cmd.Value))

	ctx := context.Background()
	switch cmd.Op {
	case "PUT":
		var opts []store.Option
		if cmd.TTL > 0 {
			opts = append(opts, store.WithTTL(cmd.TTL))
		}
		if err := f.store.Put(ctx, cmd.Key, cmd.Value, opts...); err != nil {
			fmt.Printf("FSM.Apply: Put failed: %v\n", err)
			return err
		}
		fmt.Printf("FSM.Apply: Successfully put key: %s\n", cmd.Key)
		return nil

	case "DELETE":
		if err := f.store.Delete(ctx, cmd.Key); err != nil {
			fmt.Printf("FSM.Apply: Delete failed: %v\n", err)
			return err
		}
		fmt.Printf("FSM.Apply: Successfully deleted key: %s\n", cmd.Key)
		return nil

	default:
		fmt.Printf("FSM.Apply: Unknown operation: %s\n", cmd.Op)
		return fmt.Errorf("unknown operation: %s", cmd.Op)
	}
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	fmt.Println("FSM.Snapshot: Creating snapshot...")

	snapshotter, ok := f.store.(store.Snapshotter)
	if !ok {
		fmt.Println("Store doesn't support snapshots, using empty snapshot")
		return &simpleSnapshot{}, nil
	}

	data, err := snapshotter.Export()
	if err != nil {
		fmt.Printf("FSM.Snapshot: Export failed: %v\n", err)
		return &simpleSnapshot{}, nil
	}

	fmt.Printf("FSM.Snapshot: Created snapshot with %d bytes\n", len(data))
	return &KVSnapshot{data: data}, nil
}

func (f *FSM) Restore(rc io.ReadCloser) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	defer rc.Close()

	fmt.Println("FSM.Restore: Restoring from snapshot...")

	snapshotter, ok := f.store.(store.Snapshotter)
	if !ok {
		fmt.Println("Store doesn't support snapshots, skipping restore")
		return nil
	}

	// HMM.... I'll think about this later..... Probably....
	// data, err := io.ReadAll(rc)
	// if err != nil {
	// 	return fmt.Errorf("failed to read snapshot: %v", err)
	// }

	// if err := snapshotter.Import(data); err != nil {
	// 	return fmt.Errorf("failed to import snapshot: %v", err)
	// }

	data, err := io.ReadAll(rc)
	if err != nil {
		fmt.Printf("FSM.Restore: Failed to read snapshot: %v\n", err)
		return fmt.Errorf("failed to read snapshot: %v", err)
	}

	if len(data) == 0 {
		fmt.Println("FSM.Restore: Empty snapshot, nothing to restore")
		return nil
	}

	if !json.Valid(data) {
		fmt.Println("FSM.Restore: Invalid JSON in snapshot, using empty data")
		return nil
	}

	if err := snapshotter.Import(data); err != nil {
		fmt.Printf("FSM.Restore: Import failed: %v\n", err)
		return fmt.Errorf("failed to import snapshot: %v", err)
	}

	fmt.Printf("FSM.Restore: Successfully restored snapshot (%d bytes)\n", len(data))
	return nil
}

type KVSnapshot struct {
	data []byte
}

func (s *KVSnapshot) Persist(sink raft.SnapshotSink) error {
	fmt.Printf("KVSnapshot.Persist: Writing %d bytes to snapshot\n", len(s.data))

	if len(s.data) == 0 {
		fmt.Println("KVSnapshot.Persist: No data to persist, writing empty snapshot")
		_, err := sink.Write([]byte("{}"))
		if err != nil {
			sink.Cancel()
			return err
		}
		return sink.Close()
	}

	if _, err := sink.Write(s.data); err != nil {
		fmt.Printf("KVSnapshot.Persist: Write failed: %v\n", err)
		sink.Cancel()
		return err
	}

	fmt.Println("KVSnapshot.Persist: Successfully wrote snapshot data")
	return sink.Close()
}

func (s *KVSnapshot) Release() {
	fmt.Println("KVSnapshot.Release: Releasing snapshot resources")
	s.data = nil
}

type simpleSnapshot struct{}

func (s *simpleSnapshot) Persist(sink raft.SnapshotSink) error {
	fmt.Println("simpleSnapshot.Persist: Creating empty snapshot")

	emptyData := map[string]interface{}{
		"message":   "empty snapshot - store doesn't support snapshots",
		"timestamp": time.Now().Format(time.RFC3339),
	}

	data, err := json.Marshal(emptyData)
	if err != nil {
		sink.Cancel()
		return err
	}

	if _, err := sink.Write(data); err != nil {
		sink.Cancel()
		return err
	}

	fmt.Println("simpleSnapshot.Persist: Empty snapshot created")
	return sink.Close()
}

func (s *simpleSnapshot) Release() {
	fmt.Println("simpleSnapshot.Release: No resources to release")
}
