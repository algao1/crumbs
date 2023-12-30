package lru

import (
	"container/list"
	"sync"
	"sync/atomic"
	"time"
)

type Cache struct {
	mu sync.RWMutex

	maxEntries int
	ttl        time.Duration

	ll    *list.List
	cache map[any]*list.Element
	stats Stats
}

type Stats struct {
	Hits    uint64
	Misses  uint64
	Evicted uint64
}

type entry struct {
	key          any
	value        any
	lastAccessed time.Time
}

func NewCache(maxEntries int, ttl time.Duration) *Cache {
	c := &Cache{
		maxEntries: maxEntries,
		ttl:        ttl,
		ll:         list.New(),
		cache:      make(map[any]*list.Element),
	}
	c.periodicallyEvict()
	return c
}

func (c *Cache) Add(key, value any) {
	c.mu.Lock()
	defer c.mu.Unlock()

	newEntry := entry{key: key, value: value, lastAccessed: time.Now()}

	if ele, ok := c.cache[key]; ok {
		ele.Value = newEntry
		c.ll.MoveToFront(ele)
		return
	}
	c.ll.PushFront(newEntry)
	c.cache[key] = c.ll.Front()

	if c.cacheSizeExceeded() {
		c.removeElement(c.ll.Back())
	}
}

func (c *Cache) Get(key any) (any, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	v, ok := c.cache[key]
	if !ok {
		atomic.AddUint64(&c.stats.Misses, 1)
		return nil, false
	}
	atomic.AddUint64(&c.stats.Hits, 1)

	c.ll.MoveToFront(v)
	return v.Value.(entry).value, true
}

func (c *Cache) Remove(key any) {
	c.mu.Lock()
	defer c.mu.Unlock()

	v, ok := c.cache[key]
	if !ok {
		return
	}
	c.removeElement(v)
}

func (c *Cache) Head(n int) []any {
	c.mu.RLock()
	defer c.mu.RUnlock()

	ret := make([]any, 0, n)
	cur := c.ll.Front()
	for i := 0; i < n && cur != nil; i++ {
		ret = append(ret, cur.Value.(entry).value)
		cur = cur.Next()
	}
	return ret
}

func (c *Cache) Stats() Stats {
	ret := Stats{
		Hits:    atomic.LoadUint64(&c.stats.Hits),
		Misses:  atomic.LoadUint64(&c.stats.Misses),
		Evicted: atomic.LoadUint64(&c.stats.Evicted),
	}
	return ret
}

func (c *Cache) ResetStats() {
	atomic.StoreUint64(&c.stats.Hits, 0)
	atomic.StoreUint64(&c.stats.Misses, 0)
	atomic.StoreUint64(&c.stats.Evicted, 0)
}

func (c *Cache) removeElement(e *list.Element) {
	if e == nil {
		return
	}

	c.ll.Remove(e)
	delete(c.cache, e.Value.(entry).key)
	atomic.AddUint64(&c.stats.Evicted, 1)
}

func (c *Cache) cacheSizeExceeded() bool {
	if c.maxEntries == 0 {
		return false
	}
	return len(c.cache) > c.maxEntries
}

func (c *Cache) periodicallyEvict() {
	if c.ttl == 0 {
		return
	}
	go func() {
		t := time.NewTicker(1 * time.Second)
		for {
			<-t.C
			c.mu.Lock()
			last := c.ll.Back()
			for last != nil && c.entryExpired(last.Value.(entry)) {
				cur := last
				last = last.Prev()
				c.removeElement(cur)
			}
			c.mu.Unlock()
		}
	}()
}

func (c *Cache) entryExpired(e entry) bool {
	return !e.lastAccessed.Add(c.ttl).After(time.Now())
}
