package dekvsraft

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/VT0x00/dekvs/internal/store"
	"github.com/hashicorp/raft"
)

func TestNewFSM(t *testing.T) {
	s := store.NewStore()
	fsm := NewFSM(s, nil, nil)

	if fsm == nil {
		t.Fatal("NewFSM() returned nil")
	}
	if fsm.store == nil {
		t.Error("NewFSM() did not initialize store")
	}
}

func TestFSM_ApplyPut(t *testing.T) {
	s := store.NewStore()
	fsm := NewFSM(s, nil, nil)

	cmd := store.Command{
		Op:    "put",
		Key:   "testkey",
		Value: []byte("testvalue"),
	}

	data, err := json.Marshal(cmd)
	if err != nil {
		t.Fatalf("Failed to marshal command: %v", err)
	}

	log := &raft.Log{
		Data: data,
	}

	fsm.Apply(log)

	got, found := s.Get("testkey")
	if !found {
		t.Error("Apply() did not store the value")
	}
	if string(got) != "testvalue" {
		t.Errorf("Apply() stored %q, want %q", got, "testvalue")
	}
}

func TestFSM_ApplyDelete(t *testing.T) {
	s := store.NewStore()
	fsm := NewFSM(s, nil, nil)

	s.Set("testkey", []byte("testvalue"))

	cmd := store.Command{
		Op:  "delete",
		Key: "testkey",
	}

	data, err := json.Marshal(cmd)
	if err != nil {
		t.Fatalf("Failed to marshal command: %v", err)
	}

	log := &raft.Log{
		Data: data,
	}

	fsm.Apply(log)

	_, found := s.Get("testkey")
	if found {
		t.Error("Apply() did not delete the key")
	}
}

func TestFSM_ApplyBatchPut(t *testing.T) {
	s := store.NewStore()
	fsm := NewFSM(s, nil, nil)

	cmd := store.Command{
		Op: "batchPut",
		Pairs: []store.KeyValue{
			{Key: "key1", Value: []byte("value1")},
			{Key: "key2", Value: []byte("value2")},
		},
	}

	data, err := json.Marshal(cmd)
	if err != nil {
		t.Fatalf("Failed to marshal command: %v", err)
	}

	log := &raft.Log{
		Data: data,
	}

	fsm.Apply(log)

	got, found := s.Get("key1")
	if !found {
		t.Error("Apply() did not store key1")
	}
	if string(got) != "value1" {
		t.Errorf("Apply() stored key1 = %q, want %q", got, "value1")
	}

	got, found = s.Get("key2")
	if !found {
		t.Error("Apply() did not store key2")
	}
	if string(got) != "value2" {
		t.Errorf("Apply() stored key2 = %q, want %q", got, "value2")
	}
}

func TestFSM_ApplyInvalidCommand(t *testing.T) {
	s := store.NewStore()
	fsm := NewFSM(s, nil, nil)

	log := &raft.Log{
		Data: []byte("invalid json"),
	}

	result := fsm.Apply(log)
	if result != nil {
		t.Error("Apply() should return nil for invalid command")
	}
}

func TestFSM_Snapshot(t *testing.T) {
	s := store.NewStore()
	s.Set("key1", []byte("value1"))
	s.Set("key2", []byte("value2"))

	fsm := NewFSM(s, nil, nil)

	snapshot, err := fsm.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot() error: %v", err)
	}

	if snapshot == nil {
		t.Fatal("Snapshot() returned nil")
	}
}

func TestFSM_Restore(t *testing.T) {
	s := store.NewStore()
	fsm := NewFSM(s, nil, nil)

	data := map[string][]byte{
		"key1": []byte("value1"),
		"key2": []byte("value2"),
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		t.Fatalf("Failed to marshal data: %v", err)
	}

	rc := &readCloser{bytes.NewReader(jsonData)}
	err = fsm.Restore(rc)
	if err != nil {
		t.Fatalf("Restore() error: %v", err)
	}

	got, found := s.Get("key1")
	if !found {
		t.Error("Restore() did not restore key1")
	}
	if string(got) != "value1" {
		t.Errorf("Restore() restored key1 = %q, want %q", got, "value1")
	}

	got, found = s.Get("key2")
	if !found {
		t.Error("Restore() did not restore key2")
	}
	if string(got) != "value2" {
		t.Errorf("Restore() restored key2 = %q, want %q", got, "value2")
	}
}

func TestFSM_RestoreInvalidData(t *testing.T) {
	s := store.NewStore()
	fsm := NewFSM(s, nil, nil)

	rc := &readCloser{bytes.NewReader([]byte("invalid json"))}
	err := fsm.Restore(rc)
	if err == nil {
		t.Error("Restore() should return error for invalid JSON")
	}
}

type readCloser struct {
	*bytes.Reader
}

func (r *readCloser) Close() error {
	return nil
}
