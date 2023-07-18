package lsm

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"time"
)

const (
	MEM_TABLE_SIZE = 1024 * 1024 * 16 // 16 MB
	MAX_MEM_TABLES = 4

	DEFAULT_SPARSENESS = 16
	DEFAULT_ERROR_PCT  = 0.01
)

// TODO:
// - add compaction
// - add a cache for SSTable to improve sequential reads
// - add a WAL
// - add better logging

type Memtable interface {
	Find(key string) ([]byte, bool)
	Insert(key string, val []byte)
	Traverse(f func(k string, v []byte))
	Size() int
}

type SSTable struct {
	FileSize    int
	Index       *SparseIndex
	BloomFilter *BloomFilter
	DataFile    io.ReaderAt
}

type LSMTree struct {
	mu   sync.RWMutex
	ssMu sync.RWMutex // guards ssCounter and ssTables.

	dir       string
	tables    []Memtable
	ssCounter int
	ssTables  []SSTable

	flusherCloser chan struct{}

	// Options.
	sparseness int
	errorPct   float64
}

func NewLSMTree(dir string, options ...LSMOption) (*LSMTree, error) {
	lt := &LSMTree{
		dir:           dir,
		tables:        []Memtable{NewAATree()},
		ssTables:      make([]SSTable, 0),
		flusherCloser: make(chan struct{}),
		sparseness:    DEFAULT_SPARSENESS,
		errorPct:      DEFAULT_ERROR_PCT,
	}
	for _, opt := range options {
		lt = opt(lt)
	}

	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("unable to initialize directory: %w", err)
	}

	if err := lt.loadSSTables(); err != nil {
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

	lt.ssMu.RLock()
	defer lt.ssMu.RUnlock()

	for _, ss := range lt.ssTables {
		b, found, err := lt.findInSSTable(ss, key)
		if err != nil {
			return nil, fmt.Errorf("unable to search in SSTables: %w", err)
		}
		if found {
			return b, nil
		}
	}
	return []byte{}, nil
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
		if err := lt.flush(t); err != nil {
			return fmt.Errorf("unable to flush and close db: %w", err)
		}
	}
	lt.tables = []Memtable{NewAATree()}
	return nil
}

func (lt *LSMTree) flushPeriodically() {
	t := time.NewTicker(time.Second)
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
				err := lt.flush(mt)
				if err != nil {
					// TODO: make this better.
					fmt.Println("failed to flush periodically", err)
				}
			}

			lt.mu.Lock()
			lt.tables = lt.tables[n-MAX_MEM_TABLES:]
			lt.mu.Unlock()
		}
	}
}

// findInSSTable expects caller to acquire read lock on SSTables.
func (lt *LSMTree) findInSSTable(ss SSTable, key string) ([]byte, bool, error) {
	if !ss.BloomFilter.In([]byte(key)) {
		return nil, false, nil
	}

	offset, maxOffset := ss.Index.GetOffsets(key)
	if maxOffset < 0 {
		maxOffset = ss.FileSize
	}

	const i64Size = 8

	for offset < maxOffset {
		kb, err := readBytes(ss.DataFile, int64(offset))
		if err != nil {
			return nil, false, fmt.Errorf("unable to read bytes: %w", err)
		}
		offset += i64Size + len(kb)

		vb, err := readBytes(ss.DataFile, int64(offset))
		if err != nil {
			return nil, false, fmt.Errorf("unable to read bytes: %w", err)
		}
		offset += i64Size + len(vb)

		if key == string(kb) {
			return vb, true, nil
		}
	}

	return nil, false, nil
}

// flush expects caller to hold no locks.
func (lt *LSMTree) flush(mt Memtable) error {
	lt.ssMu.Lock()
	dataPath := filepath.Join(lt.dir, fmt.Sprintf("lsm-%d.data", lt.ssCounter))
	indexPath := filepath.Join(lt.dir, fmt.Sprintf("lsm-%d.index", lt.ssCounter))
	bloomPath := filepath.Join(lt.dir, fmt.Sprintf("lsm-%d.bloom", lt.ssCounter))
	lt.ssCounter++
	lt.ssMu.Unlock()

	dataFile, err := os.OpenFile(dataPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("unable to flush: %w", err)
	}
	defer dataFile.Close()

	sparseIndex := NewSparseIndex()
	bf, err := NewBloomFilter(MEM_TABLE_SIZE/128, lt.errorPct)
	if err != nil {
		return fmt.Errorf("unable to create bloom filter: %w", err)
	}
	offset := 0
	iter := 0

	mt.Traverse(func(k string, v []byte) {
		if len(v) == 0 {
			return
		}

		// TODO: Maybe exit early on fail?
		kn, _ := flushBytes(dataFile, []byte(k))
		vn, _ := flushBytes(dataFile, v)
		bf.Add([]byte(k))

		if iter%lt.sparseness == 0 {
			sparseIndex.Append(recordOffset{
				Key:    k,
				Offset: offset,
			})
		}

		offset += kn + vn
		iter++
	})

	if err = sparseIndex.Encode(indexPath); err != nil {
		return fmt.Errorf("unable to flush sparse index: %w", err)
	}

	if err = bf.Encode(bloomPath); err != nil {
		return fmt.Errorf("unable to flush bloom filter: %w", err)
	}

	dataFile, err = os.Open(dataPath)
	if err != nil {
		return fmt.Errorf("unable to open data file: %w", err)
	}

	lt.ssMu.Lock()
	lt.ssTables = append(lt.ssTables, SSTable{
		FileSize:    offset,
		Index:       sparseIndex,
		BloomFilter: bf,
		DataFile:    dataFile,
	})
	lt.ssMu.Unlock()

	return nil
}

type ssFiles struct {
	dataFiles  []string
	indexFiles []string
	bloomFiles []string
}

func (lt *LSMTree) getFiles() (ssFiles, error) {
	dataFiles, err := filepath.Glob(filepath.Join(lt.dir, "lsm-*.data"))
	if err != nil {
		return ssFiles{}, fmt.Errorf("unable to glob data files: %w", err)
	}

	indexFiles, err := filepath.Glob(filepath.Join(lt.dir, "lsm-*.index"))
	if err != nil {
		return ssFiles{}, fmt.Errorf("unable to glob index files: %w", err)
	}

	bloomFiles, err := filepath.Glob(filepath.Join(lt.dir, "lsm-*.bloom"))
	if err != nil {
		return ssFiles{}, fmt.Errorf("unable to glob bloom files: %w", err)
	}

	return ssFiles{
		dataFiles:  dataFiles,
		indexFiles: indexFiles,
		bloomFiles: bloomFiles,
	}, nil
}

func (lt *LSMTree) loadSSTables() error {
	ssFiles, err := lt.getFiles()
	if err != nil {
		return fmt.Errorf("unable to load sstables: %w", err)
	}

	for i := range ssFiles.dataFiles {
		df, err := os.Open(ssFiles.dataFiles[i])
		if err != nil {
			return fmt.Errorf("unable to open data file: %w", err)
		}

		fi, err := os.Stat(ssFiles.dataFiles[i])
		if err != nil {
			return fmt.Errorf("unable to stat data file: %w", err)
		}

		sparseIndex := NewSparseIndex()
		err = sparseIndex.Decode(ssFiles.indexFiles[i])
		if err != nil {
			return fmt.Errorf("unable to decode sparse index: %w", err)
		}

		bf, _ := NewBloomFilter(1, 1)
		err = bf.Decode(ssFiles.bloomFiles[i])
		if err != nil {
			return fmt.Errorf("unable to decode bloom filter: %w", err)
		}

		lt.ssTables = append(lt.ssTables, SSTable{
			FileSize:    int(fi.Size()),
			DataFile:    df,
			Index:       sparseIndex,
			BloomFilter: bf,
		})
	}

	n := len(ssFiles.dataFiles)
	if n == 0 {
		return nil
	}

	// TODO: move to own function.
	re := regexp.MustCompile(`\d+`)
	match := re.FindString(ssFiles.dataFiles[n-1])
	ssCounter, err := strconv.Atoi(match)
	if err != nil {
		return fmt.Errorf("could not get latest ss id: %w", err)
	}
	lt.ssCounter = ssCounter + 1

	return nil
}
