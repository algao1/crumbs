package lsm

import (
	"fmt"
	"os"
	"sync"
	"time"
)

const (
	MEM_TABLE_SIZE = 1024 * 1024 * 16 // 16 MB
	MAX_MEM_TABLES = 4

	DEFAULT_SPARSENESS = 16
	DEFAULT_ERROR_PCT  = 0.05
)

// TODO:
// - add a WAL
// - add better logging

type LSMTree struct {
	mu sync.RWMutex

	tables        []Memtable
	stm           *SSTManager
	flusherCloser chan struct{}
}

type Memtable interface {
	Find(key string) ([]byte, bool)
	Insert(key string, val []byte)
	Traverse(f func(k string, v []byte))
	Size() int
	Nodes() int
}

func NewLSMTree(dir string, options ...LSMOption) (*LSMTree, error) {
	lt := &LSMTree{
		tables: []Memtable{NewAATree()},
		stm: NewSSTManager(dir, SSTMOptions{
			sparseness: DEFAULT_SPARSENESS,
			errorPct:   DEFAULT_ERROR_PCT,
		}),
		flusherCloser: make(chan struct{}),
	}
	for _, opt := range options {
		lt = opt(lt)
	}

	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("unable to initialize directory: %w", err)
	}

	if err := lt.stm.Load(); err != nil {
		return nil, fmt.Errorf("unable to load SSTables from disk: %w", err)
	}

	go lt.flushPeriodically()

	return lt, nil
}

func (lt *LSMTree) Put(key string, val []byte) {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	curTable := lt.tables[len(lt.tables)-1]
	curTable.Insert(key, val)

	if curTable.Size() > MEM_TABLE_SIZE {
		curTable = NewAATree()
		lt.tables = append(lt.tables, curTable)
	}
}

func (lt *LSMTree) Get(key string) ([]byte, error) {
	// Search tables in reverse chronological order.
	lt.mu.RLock()
	for i := len(lt.tables) - 1; i >= 0; i-- {
		v, found := lt.tables[i].Find(key)
		if found {
			lt.mu.RUnlock()
			return v, nil
		}
	}
	lt.mu.RUnlock()

	return lt.stm.Find(key)
}

func (lt *LSMTree) Delete(key string) {
	lt.Put(key, nil)
}

// Close flushes all memtables to disk.
func (lt *LSMTree) Close() error {
	lt.flusherCloser <- struct{}{}

	lt.mu.Lock()
	defer lt.mu.Unlock()

	toFlush := lt.tables
	for _, t := range toFlush {
		if err := lt.stm.Add(t); err != nil {
			return fmt.Errorf("unable to flush and close db: %w", err)
		}
	}
	lt.tables = []Memtable{NewAATree()}
	return nil
}

func (lt *LSMTree) Compact() {
	lt.stm.Compact()
}

func (lt *LSMTree) flushPeriodically() {
	t := time.NewTicker(5 * time.Second)
	for {
		select {
		case <-lt.flusherCloser:
			return
		case <-t.C:
			lt.mu.Lock()
			n := len(lt.tables)
			var mts []Memtable
			if n > MAX_MEM_TABLES {
				mts = lt.tables[:n-MAX_MEM_TABLES]
			} else {
				lt.mu.Unlock()
				continue
			}
			lt.mu.Unlock()

			for _, mt := range mts {
				err := lt.stm.Add(mt)
				if err != nil {
					panic(fmt.Errorf("failed to flush periodically: %w", err))
				}
			}

			lt.mu.Lock()
			lt.tables = lt.tables[n-MAX_MEM_TABLES:]
			lt.mu.Unlock()
		}
	}
}
