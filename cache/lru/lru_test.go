package lru

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestAddGetEvicted(t *testing.T) {
	c := NewCache(5, 0, nil)

	c.Add("a", nil)
	c.Add("b", nil)
	c.Add("c", nil)
	c.Add("d", nil)
	c.Add("e", nil)
	c.Add("f", nil)

	vals := c.Head(10)
	assert.Len(t, vals, 5)

	_, found := c.Get("a")
	assert.False(t, found)
	_, found = c.Get("b")
	assert.True(t, found)

	c.Add("b", nil)
	c.Add("a", nil)
	_, found = c.Get("b")
	assert.True(t, found)
	_, found = c.Get("c")
	assert.False(t, found)
}

func TestMoveToFront(t *testing.T) {
	c := NewCache(5, 0, nil)
	c.Add("a", "a")
	c.Add("b", "b")
	c.Add("c", "c")
	c.Add("d", "d")
	c.Add("e", "e")

	vals := c.Head(5)
	assert.Equal(t, vals[0].(string), "e")
	assert.Equal(t, vals[1].(string), "d")
	assert.Equal(t, vals[2].(string), "c")

	c.Add("b", "b")
	vals = c.Head(5)
	assert.Equal(t, vals[0].(string), "b")
	assert.Equal(t, vals[1].(string), "e")
	assert.Equal(t, vals[2].(string), "d")
}

func TestRemove(t *testing.T) {
	c := NewCache(0, 0, nil)
	c.Add("a", "a")
	c.Add("b", "b")
	_, found := c.Get("a")
	assert.True(t, found)

	c.Remove("a")
	_, found = c.Get("a")
	assert.False(t, found)
	_, found = c.Get("b")
	assert.True(t, found)
}

func TestTTL(t *testing.T) {
	c := NewCache(0, 1*time.Second, nil)
	c.Add("a", "a")
	c.Add("b", "b")

	vals := c.Head(2)
	assert.Len(t, vals, 2)

	time.Sleep(1 * time.Second)
	vals = c.Head(2)
	assert.Len(t, vals, 0)
}

func TestEvictFunc(t *testing.T) {
	var counter int64
	onEvict := func(k, v any) {
		atomic.AddInt64(&counter, 1)
	}
	c := NewCache(0, 1*time.Second, onEvict)
	for i := 0; i < 16; i++ {
		c.Add(i, i)
	}
	time.Sleep(2 * time.Second)
	assert.Equal(t, 16, int(counter))
}
