package store

import (
	"encoding/json"
	"sync"

	"github.com/hashicorp/raft"
)

type Command struct {
	Op       string           `json:"op"`
	Key      string           `json:"key,omitempty"`
	Value    []byte           `json:"value,omitempty"`
	Keys     []string         `json:"keys,omitempty"`
	Pairs    []KeyValue       `json:"pairs,omitempty"`
}

type KeyValue struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
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
		return nil
	case "delete":
		_, existed := s.kv[c.Key]
		delete(s.kv, c.Key)
		return existed
	case "batchPut":
		inserted := 0
		for _, pair := range c.Pairs {
			s.kv[pair.Key] = pair.Value
			inserted++
		}
		return inserted
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

func (s *Store) Delete(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, existed := s.kv[key]
	delete(s.kv, key)
	return existed
}

func (s *Store) GetBatch(keys []string) (map[string][]byte, []string) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	values := make(map[string][]byte)
	notFound := []string{}

	for _, key := range keys {
		if v, ok := s.kv[key]; ok {
			values[key] = v
		} else {
			notFound = append(notFound, key)
		}
	}

	return values, notFound
}

func (s *Store) PutBatch(pairs []KeyValue) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	inserted := 0
	for _, pair := range pairs {
		s.kv[pair.Key] = pair.Value
		inserted++
	}
	return inserted
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

func (s *Store) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.kv)
}
