package dekvsraft

import (
	"encoding/json"
	"io"
	"log"

	"github.com/githubVladimirT/dekvs/internal/store"
	"github.com/hashicorp/raft"
)

type FSM struct {
	store       *store.Store
	raft        *raft.Raft
	onApplyHook func()
}

func NewFSM(s *store.Store, r *raft.Raft, hook func()) *FSM {
	return &FSM{store: s, raft: r, onApplyHook: hook}
}

func (f *FSM) Apply(l *raft.Log) interface{} {
	var c store.Command
	if err := json.Unmarshal(l.Data, &c); err != nil {
		log.Printf("Failed to unmarshal command: %v", err)
		return nil
	}

	var result interface{}
	switch c.Op {
	case "put":
		f.store.Apply(l)
		result = nil
	case "delete":
		result = f.store.Apply(l)
	case "batchPut":
		result = f.store.Apply(l)
	}

	// Call the hook after successful apply to update metrics
	if f.onApplyHook != nil {
		f.onApplyHook()
	}

	return result
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	data := f.store.GetData()
	return &FSSnapshot{data}, nil
}

func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	var data map[string][]byte
	if err := json.NewDecoder(rc).Decode(&data); err != nil {
		return err
	}

	f.store.RestoreData(data)
	return nil
}

func (f *FSM) SetRaft(r *raft.Raft) {
	f.raft = r
}
