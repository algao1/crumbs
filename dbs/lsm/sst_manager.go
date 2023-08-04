package lsm

import (
	"container/heap"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"sync"

	"golang.org/x/exp/slog"
)

const (
	WR_FLAGS = os.O_APPEND | os.O_CREATE | os.O_WRONLY
)

type SSTManager struct {
	mu sync.RWMutex

	writeMu   sync.RWMutex
	writeable bool

	dir       string
	ssTables  [][]SSTable
	ssCounter int

	logger *slog.Logger

	// options.
	sparseness int
	errorPct   float64
}

type SSTMOptions struct {
	sparseness int
	errorPct   float64
}

type SSTable struct {
	ID       int
	FileSize int

	Meta        *Meta
	Index       *SparseIndex
	BloomFilter *BloomFilter
	DataFile    io.ReaderAt
}

func NewSSTManager(dir string, logger *slog.Logger, opts SSTMOptions) *SSTManager {
	sm := &SSTManager{
		dir:        dir,
		ssTables:   make([][]SSTable, 1),
		writeable:  true,
		sparseness: opts.sparseness,
		errorPct:   opts.errorPct,
		logger:     logger,
	}
	sm.ssTables[0] = make([]SSTable, 0)
	return sm
}

// Add adds and writes a memtable to disk as a SSTable. Requires
// that the memtable is not the active (most recent) memtable.
func (sm *SSTManager) Add(mt Memtable) error {
	sm.writeMu.RLock()
	if !sm.writeable {
		sm.writeMu.RUnlock()
		return InProgressError{}
	}
	sm.writeMu.RUnlock()

	sm.mu.Lock()
	curCounter := sm.ssCounter
	sm.ssCounter++
	sm.mu.Unlock()

	dataPath := filepath.Join(sm.dir, fmt.Sprintf("lsm-%d.data", curCounter))
	dataFile, err := os.OpenFile(dataPath, WR_FLAGS, 0644)
	if err != nil {
		return fmt.Errorf("unable to flush: %w", err)
	}
	defer dataFile.Close()

	si := NewSparseIndex()
	bf, err := NewBloomFilter(mt.Nodes(), sm.errorPct)
	if err != nil {
		return fmt.Errorf("unable to create bloom filter: %w", err)
	}
	meta := &Meta{Items: mt.Nodes()}

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

		if iter%sm.sparseness == 0 {
			si.Append(recordOffset{Key: k, Offset: offset})
		}

		offset += kn + vn
		iter++
	})

	encodeFiles(sm.dir, curCounter, meta, si, bf)

	dataFile, err = os.Open(dataPath)
	if err != nil {
		return fmt.Errorf("unable to open data file: %w", err)
	}

	sm.mu.Lock()
	sm.ssTables[0] = append(sm.ssTables[0], SSTable{
		ID:          curCounter,
		FileSize:    offset,
		Meta:        meta,
		Index:       si,
		BloomFilter: bf,
		DataFile:    dataFile,
	})
	sm.mu.Unlock()

	return nil
}

func (sm *SSTManager) Find(key string) ([]byte, error) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	for _, level := range sm.ssTables {
		for _, ss := range level {
			b, found, err := findInSSTable(ss, key)
			if err != nil {
				return nil, fmt.Errorf("unable to search in SSTables: %w", err)
			}
			if found {
				return b, nil
			}
		}
	}

	return []byte{}, nil
}

func (sm *SSTManager) Load() error {
	ssFiles, err := getFiles(sm.dir)
	if err != nil {
		return fmt.Errorf("unable to load sstables: %w", err)
	}

	for i := range ssFiles.dataFiles {
		meta := &Meta{}
		if err := meta.Decode(ssFiles.metaFiles[i]); err != nil {
			return fmt.Errorf("unable to open meta file: %w", err)
		}

		df, err := os.Open(ssFiles.dataFiles[i])
		if err != nil {
			return fmt.Errorf("unable to open data file: %w", err)
		}

		fi, err := os.Stat(ssFiles.dataFiles[i])
		if err != nil {
			return fmt.Errorf("unable to stat data file: %w", err)
		}

		sparseIndex := NewSparseIndex()
		if err = sparseIndex.Decode(ssFiles.indexFiles[i]); err != nil {
			return fmt.Errorf("unable to decode sparse index: %w", err)
		}

		bf, _ := NewBloomFilter(1, 1)
		if err = bf.Decode(ssFiles.bloomFiles[i]); err != nil {
			return fmt.Errorf("unable to decode bloom filter: %w", err)
		}

		for meta.Level >= len(sm.ssTables) {
			sm.ssTables = append(sm.ssTables, make([]SSTable, 0))
		}
		sm.ssTables[meta.Level] = append(sm.ssTables[meta.Level], SSTable{
			ID:          ssFiles.ids[i],
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
	sm.ssCounter = ssFiles.ids[len(ssFiles.ids)-1] + 1

	return nil
}

// Compact calls a more fine-grained compactTables under the hood.
// It must ensure that
//   - the SSTable counter is properly updated
//   - that writeable is set to false so sm.Add operations will fail
//
// We only acquire RLock during compaction to allow for concurrent reads.
// Once we are finished, we acquire a lock to first add the new table,
// then remove stale tables.
func (sm *SSTManager) Compact() {
	sm.mu.Lock()
	if len(sm.ssTables[0]) > 0 {
		newID := sm.ssCounter
		sm.ssCounter++
		sm.mu.Unlock()

		sm.logger.Info("compaction in progress")

		sm.writeMu.Lock()
		sm.writeable = false
		sm.writeMu.Unlock()
		defer func() {
			sm.writeMu.Lock()
			sm.writeable = true
			sm.writeMu.Unlock()
		}()

		sm.mu.RLock()
		toCompact := sm.ssTables[0]
		newTable := sm.compactTables(newID, toCompact)
		sm.mu.RUnlock()

		// Lock and make updates to table.
		sm.mu.Lock()
		if len(sm.ssTables) <= newTable.Meta.Level {
			sm.ssTables = append(sm.ssTables, make([]SSTable, 0))
		}
		sm.ssTables[newTable.Meta.Level] = append(
			[]SSTable{newTable},
			sm.ssTables[newTable.Meta.Level]...,
		)

		for _, t := range toCompact {
			pattern := filepath.Join(sm.dir, fmt.Sprintf("lsm-%d.*", t.ID))
			toRemove, err := filepath.Glob(pattern)
			if err != nil {
				panic(err)
			}
			for _, f := range toRemove {
				os.Remove(f)
			}
		}
		sm.mu.Unlock()

		sm.logger.Info(
			"compaction finished",
			"tablesCompacted", len(toCompact),
			"newTableLevel", newTable.Meta.Level,
		)
		return
	}
	sm.mu.Unlock()
}

func (sm *SSTManager) compactTables(newID int, tables []SSTable) SSTable {
	kfh := make(KeyFileHeap, len(tables))
	level := tables[0].Meta.Level
	totalItems := 0

	for i, t := range tables {
		kvp, offset, _ := readKeyValue(t.DataFile, 0)
		kfh[i] = KeyFile{
			Key:    string(kvp.key),
			Value:  kvp.value,
			File:   i, // NOTE: this file does not represent the FileID.
			Offset: int(offset),
		}
		totalItems += t.Meta.Items
	}
	heap.Init(&kfh)

	dataPath := filepath.Join(sm.dir, fmt.Sprintf("lsm-%d.data", newID))
	dataFile, err := os.OpenFile(dataPath, WR_FLAGS, 0644)
	if err != nil {
		panic(fmt.Errorf("unable to flush: %w", err))
	}
	defer dataFile.Close()

	si := NewSparseIndex()
	bf, err := NewBloomFilter(totalItems, sm.errorPct)
	if err != nil {
		panic(fmt.Errorf("unable to create bloom filter: %w", err))
	}
	meta := &Meta{Level: level + 1, Items: totalItems}

	sparseness := int(math.Pow(float64(sm.sparseness), float64(level+2)))

	offset := 0
	iter := 0
	for len(kfh) > 0 {
		kf := heap.Pop(&kfh).(KeyFile)

		if iter%sparseness == 0 {
			si.Append(recordOffset{Key: string(kf.Key), Offset: offset})
		}

		kl, _ := flushBytes(dataFile, []byte(kf.Key))
		vl, _ := flushBytes(dataFile, kf.Value)
		bf.Add([]byte(kf.Key))

		if kf.Offset >= tables[kf.File].FileSize {
			continue
		}

		kvp, newOffset, _ := readKeyValue(
			tables[kf.File].DataFile,
			int64(kf.Offset),
		)

		offset += kl + vl
		iter++

		heap.Push(&kfh, KeyFile{
			Key:    string(kvp.key),
			File:   kf.File,
			Offset: int(newOffset),
		})
	}

	err = encodeFiles(sm.dir, newID, meta, si, bf)
	if err != nil {
		panic(err)
	}

	return SSTable{
		ID:          newID,
		FileSize:    offset,
		Meta:        meta,
		Index:       si,
		BloomFilter: bf,
		DataFile:    dataFile,
	}
}

// findInSSTable expects caller to acquire read lock on SSTables.
func findInSSTable(ss SSTable, key string) ([]byte, bool, error) {
	if ss.BloomFilter != nil && !ss.BloomFilter.In([]byte(key)) {
		return nil, false, nil
	}

	offset, maxOffset := ss.Index.GetOffsets(key)
	if maxOffset < 0 {
		maxOffset = ss.FileSize
	}

	for offset < maxOffset {
		kvp, newOffset, err := readKeyValue(ss.DataFile, int64(offset))
		if err != nil {
			return nil, false, fmt.Errorf("unable to find in SSTable: %w", err)
		}
		offset = int(newOffset)

		if key == string(kvp.key) {
			return kvp.value, true, nil
		}
	}

	return nil, false, nil
}

type ssFiles struct {
	ids        []int
	metaFiles  []string
	dataFiles  []string
	indexFiles []string
	bloomFiles []string
}

func encodeFiles(dir string, id int, meta *Meta, si *SparseIndex, bf *BloomFilter) error {
	metaPath := filepath.Join(dir, fmt.Sprintf("lsm-%d.meta", id))
	indexPath := filepath.Join(dir, fmt.Sprintf("lsm-%d.index", id))
	bloomPath := filepath.Join(dir, fmt.Sprintf("lsm-%d.bloom", id))

	if err := si.Encode(indexPath); err != nil {
		return fmt.Errorf("unable to flush sparse index: %w", err)
	}
	if err := bf.Encode(bloomPath); err != nil {
		return fmt.Errorf("unable to flush bloom filter: %w", err)
	}
	if err := meta.Encode(metaPath); err != nil {
		return fmt.Errorf("unable to flush metadata: %w", err)
	}

	return nil
}

func getFiles(dir string) (ssFiles, error) {
	metaFiles, err := filepath.Glob(filepath.Join(dir, "lsm-*.meta"))
	if err != nil {
		return ssFiles{}, fmt.Errorf("unable to glob meta files: %w", err)
	}

	dataFiles := make([]string, len(metaFiles))
	indexFiles := make([]string, len(metaFiles))
	bloomFiles := make([]string, len(metaFiles))

	sortedIDs := getSortedFileIDs(metaFiles)
	for i, id := range sortedIDs {
		metaFiles[i] = filepath.Join(dir, fmt.Sprintf("lsm-%d.meta", id))
		dataFiles[i] = filepath.Join(dir, fmt.Sprintf("lsm-%d.data", id))
		indexFiles[i] = filepath.Join(dir, fmt.Sprintf("lsm-%d.index", id))
		bloomFiles[i] = filepath.Join(dir, fmt.Sprintf("lsm-%d.bloom", id))
	}

	return ssFiles{
		ids:        sortedIDs,
		metaFiles:  metaFiles,
		dataFiles:  dataFiles,
		indexFiles: indexFiles,
		bloomFiles: bloomFiles,
	}, nil
}

func getSortedFileIDs(files []string) []int {
	fileIDs := make([]int, len(files))
	for i, df := range files {
		fileIDs[i] = getFileID(df)
	}
	sort.Ints(fileIDs)
	return fileIDs
}

func getFileID(file string) int {
	re := regexp.MustCompile("[0-9]+")
	match := re.FindString(file)
	id, _ := strconv.Atoi(match)
	return id
}

// See: https://dave.cheney.net/2014/12/24/inspecting-errors
// Trying this pattern out to see if its any good.

type InProgressError struct{}

func (ip InProgressError) Error() string { return "in progress" }