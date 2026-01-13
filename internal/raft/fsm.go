package raft

import (
	"bytes"
	"compress/gzip"
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
	// print 'hello'
	mu sync.Mutex
}

func NewFSM(store store.Store) *FSM {
	return &FSM{
		store: store,
	}
}

func (f *FSM) Apply(log *raft.Log) any { // interface{} --> any
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
		return &emptySnapshot{}, nil
	}

	data, err := snapshotter.Export()
	if err != nil {
		fmt.Printf("FSM.Snapshot: Export failed: %v\n", err)
		return &emptySnapshot{}, nil
	}

	if len(data) == 0 {
		fmt.Println("FSM.Snapshot: Store is empty, creating empty snapshot")
		return &emptySnapshot{}, nil
	}

	fmt.Printf("FSM.Snapshot: Created snapshot with %d bytes\n", len(data))
	return &raftSnapshot{data: data}, nil
}

func (f *FSM) Restore(rc io.ReadCloser) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	defer rc.Close()

	fmt.Println("FSM.Restore: Restoring from snapshot...")

	data, err := io.ReadAll(rc)
	if err != nil {
		return fmt.Errorf("failed to read snapshot: %v", err)
	}

	fmt.Printf("FSM.Restore: Read %d bytes from snapshot\n", len(data))

	if len(data) == 0 {
		fmt.Println("FSM.Restore: Empty snapshot, nothing to restore")
		return nil
	}

	var uncompressedData []byte

	if len(data) > 2 && data[0] == 0x1f && data[1] == 0x8b {
		fmt.Println("FSM.Restore: Detected gzip compressed data, decompressing...")
		gr, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return fmt.Errorf("failed to create gzip reader: %v", err)
		}
		defer gr.Close()

		uncompressedData, err = io.ReadAll(gr)
		if err != nil {
			return fmt.Errorf("failed to decompress snapshot: %v", err)
		}

		fmt.Printf("FSM.Restore: Decompressed %d -> %d bytes\n", len(data), len(uncompressedData))
	} else {
		uncompressedData = data
		fmt.Printf("FSM.Restore: Using uncompressed data (%d bytes)\n", len(uncompressedData))
	}

	snapshotter, ok := f.store.(store.Snapshotter)
	if !ok {
		return fmt.Errorf("store doesn't support snapshot restoration")
	}

	if err := snapshotter.Import(uncompressedData); err != nil {
		return fmt.Errorf("failed to import snapshot data: %v", err)
	}

	fmt.Printf("FSM.Restore: Successfully restored Raft snapshot\n")

	return nil
}

type raftSnapshot struct {
	data []byte
}

func (s *raftSnapshot) Persist(sink raft.SnapshotSink) error {
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

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)

	if _, err := gz.Write(s.data); err != nil {
		sink.Cancel()
		return fmt.Errorf("failed to compress data: %v", err)
	}

	if err := gz.Close(); err != nil {
		sink.Cancel()
		return fmt.Errorf("failed to close gzip writer: %v", err)
	}

	compressedData := buf.Bytes()
	fmt.Printf("raftSnapshot.Persist: Compressed %d -> %d bytes\n",
		len(s.data), len(compressedData))

	if _, err := sink.Write(compressedData); err != nil {
		sink.Cancel()
		return fmt.Errorf("failed to write compressed data: %v", err)
	}

	fmt.Println("raftSnapshot.Persist: Successfully wrote Raft snapshot")
	return sink.Close()

}

func (s *raftSnapshot) Release() {
	fmt.Println("KVSnapshot.Release: Releasing snapshot resources")
	s.data = nil
}

type emptySnapshot struct{}

func (s *emptySnapshot) Persist(sink raft.SnapshotSink) error {
	fmt.Println("simpleSnapshot.Persist: Creating empty snapshot")

	emptyData := map[string]any{ // interface{} --> any
		"message":   "empty snapshot - store doesn't support snapshots",
		"timestamp": time.Now().Format(time.RFC3339),
		"version":   "1.0",
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

	fmt.Println("emptySnapshot.Persist: Empty Raft snapshot created")
	return sink.Close()
}

func (s *emptySnapshot) Release() {
	fmt.Println("emptySnapshot.Release: No resources to release")
}
