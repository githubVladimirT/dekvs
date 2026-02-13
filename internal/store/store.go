package store

import (
	"encoding/json"
	"sync"

	"github.com/hashicorp/raft"
)

type Command struct {
	Op    string `json:"op"`
	Key   string `json:"key"`
	Value []byte `json:"value,omitempty"`

	PeerID   string `json:"peer_id,omitempty"`
	PeerAddr string `json:"peer_addr,omitempty"`
}

type Store struct {
	mu sync.RWMutex
	kv map[string][]byte
}

func NewStore() *Store {
	return &Store{
		kv: make(map[string][]byte),
	}
}

func (s *Store) Apply(l *raft.Log) interface{} {
	var c Command
	if err := json.Unmarshal(l.Data, &c); err != nil {
		panic("failed to unmarshal command")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	switch c.Op {
	case "put":
		s.kv[c.Key] = c.Value
	}
	return nil
}

func (s *Store) Get(key string) ([]byte, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	v, ok := s.kv[key]
	return v, ok
}

func (s *Store) Set(key string, value []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.kv[key] = value
}

func (s *Store) GetData() map[string][]byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	dst := make(map[string][]byte, len(s.kv))
	for k, v := range s.kv {
		dst[k] = v
	}
	return dst
}

func (s *Store) RestoreData(data map[string][]byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.kv = data
}
