package raft

import (
	"encoding/json"
	"io"

	"github.com/githubVladimirT/dekvs/internal/store"
	"github.com/hashicorp/raft"
)

type snapshot struct {
	Data    map[string]*store.VersionedValue
	Version uint64
}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	data, err := json.Marshal(s)
	if err != nil {
		sink.Cancel()
		return err
	}

	if _, err := sink.Write(data); err != nil {
		sink.Cancel()
		return err
	}

	return sink.Close()
}

func (s *snapshot) Release() {}
