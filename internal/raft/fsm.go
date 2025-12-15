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
		return &simpleSnapshot{}, nil
	}

	data, err := snapshotter.Export()
	if err != nil {
		fmt.Printf("FSM.Snapshot: Export failed: %v\n", err)
		return &simpleSnapshot{}, nil
	}

	if len(data) == 0 {
		fmt.Println("FSM.Snapshot: Exported data is empty!")
		return &simpleSnapshot{}, nil
	}

	var snapshotWithMeta struct {
		Data      []byte    `json:"data"`
		Timestamp time.Time `json:"timestamp"`
		Version   string    `json:"version"`
	}

	snapshotWithMeta.Data = data
	snapshotWithMeta.Timestamp = time.Now()
	snapshotWithMeta.Version = "v1"

	metaData, err := json.Marshal(snapshotWithMeta)
	if err != nil {
		fmt.Printf("FSM.Snapshot: Failed to add metadata: %v\n", err)
		metaData = data
	} else {
		fmt.Printf("FSM.Snapshot: Added metadata, total size: %d bytes\n", len(metaData))
	}

	fmt.Printf("FSM.Snapshot: Created snapshot with %d bytes\n", len(metaData))
	return &KVSnapshot{data: metaData}, nil

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

	var snapshotWithMeta struct {
		Data      []byte    `json:"data"`
		Timestamp time.Time `json:"timestamp"`
		Version   string    `json:"version"`
	}
	var snapshotData []byte

	if err := json.Unmarshal(data, &snapshotWithMeta); err == nil && snapshotWithMeta.Data != nil {
		fmt.Printf("FSM.Restore: Snapshot with metadata - Timestamp: %v, Version: %s\n",
			snapshotWithMeta.Timestamp, snapshotWithMeta.Version)
		snapshotData = snapshotWithMeta.Data
	} else {
		fmt.Println("FSM.Restore: Old snapshot format (no metadata)")
		snapshotData = data
	}

	var uncompressedData []byte
	if len(snapshotData) >= 2 && snapshotData[0] == 0x1f && snapshotData[1] == 0x8b {
		fmt.Println("FSM.Restore: Detected gzip compressed data, decompressing...")
		gr, err := gzip.NewReader(bytes.NewReader(snapshotData))
		if err != nil {
			return fmt.Errorf("failed to create gzip reader: %v", err)
		}
		defer gr.Close()

		uncompressedData, err = io.ReadAll(gr)
		if err != nil {
			return fmt.Errorf("failed to decompress snapshot: %v", err)
		}
		fmt.Printf("FSM.Restore: Decompressed %d -> %d bytes\n",
			len(snapshotData), len(uncompressedData))
	} else {
		uncompressedData = snapshotData
		fmt.Printf("FSM.Restore: Using uncompressed data (%d bytes)\n", len(uncompressedData))
	}

	snapshotter, ok := f.store.(store.Snapshotter)
	if !ok {
		return fmt.Errorf("store doesn't support snapshot restoration")
	}

	if err := snapshotter.Import(uncompressedData); err != nil {
		return fmt.Errorf("failed to import snapshot data: %v", err)
	}

	fmt.Printf("FSM.Restore: Successfully restored snapshot (%d bytes)\n", len(uncompressedData))
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
	compressionRatio := float64(len(s.data)) / float64(len(compressedData))

	fmt.Printf("KVSnapshot.Persist: Compressed %d -> %d bytes (ratio: %.2f)\n",
		len(s.data), len(compressedData), compressionRatio)

	if _, err := sink.Write(compressedData); err != nil {
		sink.Cancel()
		return fmt.Errorf("failed to write compressed data: %v", err)
	}

	fmt.Println("KVSnapshot.Persist: Successfully wrote compressed snapshot")
	return sink.Close()

}

func (s *KVSnapshot) Release() {
	fmt.Println("KVSnapshot.Release: Releasing snapshot resources")
	s.data = nil
}

type simpleSnapshot struct{}

func (s *simpleSnapshot) Persist(sink raft.SnapshotSink) error {
	fmt.Println("simpleSnapshot.Persist: Creating empty snapshot")

	emptyData := map[string]any{ // interface{} --> any
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
