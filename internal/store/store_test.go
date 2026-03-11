package store

import (
	"testing"
)

func TestNewStore(t *testing.T) {
	s := NewStore()
	if s == nil {
		t.Fatal("NewStore() returned nil")
	}
	if s.kv == nil {
		t.Error("NewStore() did not initialize kv map")
	}
}

func TestStore_SetAndGet(t *testing.T) {
	s := NewStore()

	key := "testkey"
	value := []byte("testvalue")

	s.Set(key, value)

	got, found := s.Get(key)
	if !found {
		t.Error("Get() returned found=false for existing key")
	}
	if string(got) != string(value) {
		t.Errorf("Get() returned %q, want %q", got, value)
	}
}

func TestStore_GetNonExistent(t *testing.T) {
	s := NewStore()

	_, found := s.Get("nonexistent")
	if found {
		t.Error("Get() returned found=true for non-existent key")
	}
}

func TestStore_Delete(t *testing.T) {
	s := NewStore()

	s.Set("key1", []byte("value1"))

	existed := s.Delete("key1")
	if !existed {
		t.Error("Delete() returned existed=false for existing key")
	}

	_, found := s.Get("key1")
	if found {
		t.Error("Key still exists after deletion")
	}

	existed = s.Delete("nonexistent")
	if existed {
		t.Error("Delete() returned existed=true for non-existent key")
	}
}

func TestStore_GetBatch(t *testing.T) {
	s := NewStore()
	s.Set("key1", []byte("value1"))
	s.Set("key2", []byte("value2"))
	s.Set("key3", []byte("value3"))

	values, notFound := s.GetBatch([]string{"key1", "key2", "nonexistent"})

	if len(values) != 2 {
		t.Errorf("GetBatch() returned %d values, want 2", len(values))
	}

	if len(notFound) != 1 {
		t.Errorf("GetBatch() returned %d notFound keys, want 1", len(notFound))
	}

	if string(values["key1"]) != "value1" {
		t.Errorf("GetBatch() key1 = %q, want %q", values["key1"], "value1")
	}
}

func TestStore_PutBatch(t *testing.T) {
	s := NewStore()

	pairs := []KeyValue{
		{Key: "key1", Value: []byte("value1")},
		{Key: "key2", Value: []byte("value2")},
		{Key: "key3", Value: []byte("value3")},
	}

	count := s.PutBatch(pairs)

	if count != 3 {
		t.Errorf("PutBatch() returned count %d, want 3", count)
	}

	if s.Count() != 3 {
		t.Errorf("Store count is %d, want 3", s.Count())
	}

	values, _ := s.GetBatch([]string{"key1", "key2", "key3"})
	if len(values) != 3 {
		t.Error("Not all batch-inserted keys were found")
	}
}

func TestStore_Count(t *testing.T) {
	s := NewStore()

	if s.Count() != 0 {
		t.Errorf("Count() = %d, want 0", s.Count())
	}

	s.Set("key1", []byte("value1"))
	s.Set("key2", []byte("value2"))

	if s.Count() != 2 {
		t.Errorf("Count() = %d, want 2", s.Count())
	}

	s.Delete("key1")

	if s.Count() != 1 {
		t.Errorf("Count() after delete = %d, want 1", s.Count())
	}
}

func TestStore_GetData(t *testing.T) {
	s := NewStore()

	s.Set("key1", []byte("value1"))
	s.Set("key2", []byte("value2"))

	data := s.GetData()

	if len(data) != 2 {
		t.Errorf("GetData() returned %d items, want 2", len(data))
	}

	if string(data["key1"]) != "value1" {
		t.Errorf("GetData()[key1] = %q, want %q", data["key1"], "value1")
	}
}

func TestStore_GetDataReturnsCopy(t *testing.T) {
	s := NewStore()
	s.Set("key1", []byte("value1"))

	data := s.GetData()
	data["key1"] = []byte("modified")

	original, _ := s.Get("key1")
	if string(original) != "value1" {
		t.Error("GetData() did not return a copy")
	}
}

func TestStore_RestoreData(t *testing.T) {
	s := NewStore()
	s.Set("key1", []byte("value1"))
	s.Set("key2", []byte("value2"))

	newData := map[string][]byte{
		"key3": []byte("value3"),
		"key4": []byte("value4"),
	}

	s.RestoreData(newData)

	data := s.GetData()
	if len(data) != 2 {
		t.Errorf("RestoreData() resulted in %d items, want 2", len(data))
	}

	if _, found := data["key1"]; found {
		t.Error("RestoreData() did not replace old data")
	}

	if string(data["key3"]) != "value3" {
		t.Errorf("RestoreData() failed to restore key3")
	}
}

func TestStore_ConcurrentAccess(t *testing.T) {
	s := NewStore()

	done := make(chan bool, 10)

	for i := 0; i < 10; i++ {
		go func(n int) {
			key := "key" + string(rune('a'+n))
			s.Set(key, []byte("value"))
			s.Get(key)
			s.Delete(key)
			done <- true
		}(i)
	}

	for i := 0; i < 10; i++ {
		<-done
	}
}
