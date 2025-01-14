package lsm

import (
	"fmt"
	"os"
	"sync"
	"time"

	"golang.org/x/exp/slog"
)

const (
	DEFAULT_MEM_TABLE_SIZE     = 1024 * 1024 * 64 // 64 MB
	DEFAULT_MAX_MEM_TABLES     = 4
	DEFAULT_MAX_FLUSHED_TABLES = 16
	DEFAULT_SPARSENESS         = 16
	DEFAULT_ERROR_PCT          = 0.01
	DEFAULT_FLUSH_PERIOD       = 15 * time.Second
)

// TODO:
// - better compaction schemes
// - add a WAL
// - update README.md

type LSMTree struct {
	mu     sync.RWMutex
	logger *slog.Logger

	tables []Memtable
	stm    *SSTManager

	memTableSize  int
	maxMemTables  int
	flushPeriod   time.Duration
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
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	lt := &LSMTree{
		tables: []Memtable{NewAATree()},
		stm: NewSSTManager(
			dir,
			logger,
			SSTMOptions{
				sparseness: DEFAULT_SPARSENESS,
				errorPct:   DEFAULT_ERROR_PCT,
			},
		),
		memTableSize:  DEFAULT_MEM_TABLE_SIZE,
		maxMemTables:  DEFAULT_MAX_MEM_TABLES,
		flushPeriod:   DEFAULT_FLUSH_PERIOD,
		flusherCloser: make(chan struct{}),
		logger:        logger,
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

	if curTable.Size() > lt.memTableSize {
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
	// TODO: also stop compaction here.
	lt.flusherCloser <- struct{}{}
	return lt.FlushMemory()
}

func (lt *LSMTree) FlushMemory() error {
	lt.mu.Lock()
	defer lt.mu.Unlock()

	toFlush := lt.tables
	lt.logger.Info("flushing memtables", slog.Int("tables to flush", len(toFlush)))

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
	t := time.NewTicker(lt.flushPeriod)
	for {
		select {
		case <-lt.flusherCloser:
			lt.logger.Info("periodic flush goroutine closed")
			return
		case <-t.C:
			lt.mu.Lock()

			var mts []Memtable
			numToFlush := min(
				max(len(lt.tables)-lt.maxMemTables, 0),
				DEFAULT_MAX_FLUSHED_TABLES,
			)
			numInMemory := len(lt.tables) - numToFlush

			if numToFlush > 0 {
				mts = lt.tables[:numToFlush]
			} else {
				lt.logger.Info("nothing to flush, skipping")
				lt.mu.Unlock()
				continue
			}
			lt.logger.Info("flushing memtables", slog.Int("tables to flush", numToFlush))
			lt.mu.Unlock()

			for _, mt := range mts {
				err := lt.stm.Add(mt)
				if err != nil {
					lt.logger.Warn("failed to flush periodically", "error", err)
					break
				}
			}
			lt.logger.Info("finished flushing memtables",
				slog.Int("tables flushed", numToFlush),
				slog.Int("tables in memory", numInMemory),
			)

			lt.mu.Lock()
			lt.tables = lt.tables[numToFlush:]
			lt.mu.Unlock()
		}
	}
}
