package cache

import (
	"container/list"
	"sync"
)

type Cache interface {
	Get(key string) (any, bool)
	Put(key string, value any)
	Delete(key string)
	Clear()
	Len() int
}

type LRUCache struct {
	capacity int
	cache    map[string]*list.Element
	list     *list.List
	mu       sync.RWMutex
}

type entry struct {
	key   string
	value any
}

func NewLRUCache(capacity int) *LRUCache {
	return &LRUCache{
		capacity: capacity,
		cache:    make(map[string]*list.Element),
		list:     list.New(),
	}
}

func (c *LRUCache) Get(key string) (any, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if el, ok := c.cache[key]; ok {
		c.list.MoveToFront(el)
		return el.Value.(*entry).value, true
	}
	return nil, false
}

func (c *LRUCache) Put(key string, value any) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if el, ok := c.cache[key]; ok {
		c.list.MoveToFront(el)
		el.Value.(*entry).value = value
		return
	}

	el := c.list.PushFront(&entry{key, value})
	c.cache[key] = el

	if c.list.Len() > c.capacity {
		c.removeOldest()
	}
}

func (c *LRUCache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if el, ok := c.cache[key]; ok {
		c.list.Remove(el)
		delete(c.cache, key)
	}
}

func (c *LRUCache) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cache = make(map[string]*list.Element)
	c.list.Init()
}

func (c *LRUCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.list.Len()
}

func (c *LRUCache) removeOldest() {
	el := c.list.Back()
	if el != nil {
		c.list.Remove(el)
		delete(c.cache, el.Value.(*entry).key)
	}
}
