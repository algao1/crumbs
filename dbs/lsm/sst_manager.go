package lsm

import (
	"bufio"
	"bytes"
	"container/heap"
	"crumbs/bloom"
	"fmt"
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
	WR_FLAGS = os.O_APPEND | os.O_CREATE | os.O_RDWR
)

type SSTManager struct {
	mu     sync.RWMutex
	logger *slog.Logger

	dir       string
	ssTables  [][]SSTable
	ssCounter int
	bytesPool sync.Pool

	// Options.
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
	BloomFilter *bloom.BloomFilterV2
	DataFile    *os.File
}

func NewSSTManager(dir string, logger *slog.Logger, opts SSTMOptions) *SSTManager {
	sm := &SSTManager{
		dir:      dir,
		ssTables: make([][]SSTable, 1),
		bytesPool: sync.Pool{New: func() any {
			return new([]byte)
		}},
		sparseness: opts.sparseness,
		errorPct:   opts.errorPct,
		logger:     logger,
	}
	sm.ssTables[0] = make([]SSTable, 0)
	return sm
}

// Add adds and writes a memtable to disk as a SSTable. And requires
// that the memtable is not the active (most recent) memtable.
func (sm *SSTManager) Add(mt Memtable) error {
	sm.mu.Lock()
	curCounter := sm.ssCounter
	sm.ssCounter++
	sm.mu.Unlock()

	dataPath := filepath.Join(sm.dir, fmt.Sprintf("lsm-%d.data", curCounter))
	dataFile, err := os.OpenFile(dataPath, WR_FLAGS, 0644)
	if err != nil {
		return fmt.Errorf("unable to flush: %w", err)
	}
	bw := bufferedWriter{bufio.NewWriterSize(dataFile, 1024*64)}
	defer bw.Flush()

	si := NewSparseIndex()
	bf, err := bloom.NewBloomFilterV2(mt.Nodes(), sm.errorPct)
	if err != nil {
		return fmt.Errorf("unable to create bloom filter: %w", err)
	}
	meta := &Meta{Items: mt.Nodes()}

	offset := 0
	iter := 0
	mt.Traverse(func(k string, v []byte) {
		// TODO: Maybe exit early on fail?
		n, _ := bw.writeKeyVal(k, v)
		bf.Add([]byte(k))

		if iter%sm.sparseness == 0 {
			si.Append(recordOffset{Key: k, Offset: offset})
		}

		offset += n
		iter++
	})

	err = encodeFiles(sm.dir, curCounter, meta, si, bf)
	if err != nil {
		return fmt.Errorf("unable to encode compacted files: %w", err)
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
			b, found, err := sm.findInSSTable(ss, key)
			if err != nil {
				return nil, fmt.Errorf("unable to search in SSTables: %w", err)
			}
			if found {
				return b, nil
			}
		}
	}

	return nil, nil
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

		bf, _ := bloom.NewBloomFilterV2(1, 1)
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
// After Compact is called, we guarantee that
//   - the SSTable counter is incremented if compaction occurs
//   - that writeable is set to false so sm.Add operations will raise
//     the appropriate errors while compacting
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

		sm.logger.Info("compaction: in progress")

		sm.mu.RLock()
		// TODO: temporary.
		toCompact := sm.ssTables[0]
		newTable := sm.compactTables(newID, toCompact)
		sm.mu.RUnlock()

		sm.logger.Info("compaction: finished creating new table")

		// Lock and make updates to table.
		sm.mu.Lock()
		if len(sm.ssTables) <= newTable.Meta.Level {
			sm.ssTables = append(sm.ssTables, make([]SSTable, 0))
		}
		sm.ssTables[newTable.Meta.Level] = append(
			sm.ssTables[newTable.Meta.Level],
			newTable,
		)

		// TODO: temporary.
		sm.ssTables[0] = make([]SSTable, 0)
		sm.mu.Unlock()

		for _, t := range toCompact {
			pattern := filepath.Join(sm.dir, fmt.Sprintf("lsm-%d.*", t.ID))
			toRemove, err := filepath.Glob(pattern)
			if err != nil {
				sm.logger.Error("unable to glob stale files", err)
				continue
			}
			for _, f := range toRemove {
				os.Remove(f)
			}
		}

		sm.logger.Info(
			"compaction: finished",
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
		// TODO: For better memory performance, don't load the whole chunk,
		// load smaller ones (maybe like every X intervals?).
		chunk, _ := readChunk(t.DataFile, 0, t.FileSize)
		buf := bytes.NewBuffer(chunk)

		kvp, _, _ := readKeyVal(buf)
		kfh[i] = KeyFile{
			Key:     string(kvp.key),
			Value:   kvp.value,
			FileIdx: i, // NOTE: this file does not represent the FileID.
			Reader:  buf,
		}
		totalItems += t.Meta.Items
	}
	heap.Init(&kfh)

	dataPath := filepath.Join(sm.dir, fmt.Sprintf("lsm-%d.data", newID))
	dataFile, err := os.OpenFile(dataPath, WR_FLAGS, 0644)
	if err != nil {
		sm.logger.Error("unable to open data file for compaction", err)
		return SSTable{}
	}
	bw := bufferedWriter{bufio.NewWriterSize(dataFile, 1024*64)}
	defer bw.Flush()

	si := NewSparseIndex()
	bf, err := bloom.NewBloomFilterV2(totalItems, sm.errorPct)
	if err != nil {
		sm.logger.Error("unable to create bloom filter", err)
		return SSTable{}
	}

	meta := &Meta{Level: level + 1, Items: totalItems}
	sparseness := int(math.Pow(float64(sm.sparseness), float64(level+2)))

	offset := 0
	iter := 0
	prevKeyFile := KeyFile{}

	for len(kfh) > 0 {
		keyFile := heap.Pop(&kfh).(KeyFile)

		if keyFile.Key != prevKeyFile.Key && string(keyFile.Value) != "" {
			if iter%sparseness == 0 {
				si.Append(recordOffset{
					Key:    string(prevKeyFile.Key),
					Offset: offset,
				})
			}

			n, _ := bw.writeKeyVal(prevKeyFile.Key, prevKeyFile.Value)
			bf.Add([]byte(prevKeyFile.Key))

			offset += n
			iter++
		}

		prevKeyFile = keyFile
		if keyFile.Reader.Len() == 0 {
			continue
		}
		kvp, _, _ := readKeyVal(keyFile.Reader)
		heap.Push(&kfh, KeyFile{
			Key:     string(kvp.key),
			Value:   kvp.value,
			FileIdx: keyFile.FileIdx,
			Reader:  keyFile.Reader,
		})
	}

	// We need to remember to do the last one.
	n, _ := bw.writeKeyVal(prevKeyFile.Key, prevKeyFile.Value)
	bf.Add([]byte(prevKeyFile.Key))
	offset += n

	err = encodeFiles(sm.dir, newID, meta, si, bf)
	if err != nil {
		sm.logger.Error("unable to encode compacted files", err)
		return SSTable{}
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
func (sm *SSTManager) findInSSTable(ss SSTable, key string) ([]byte, bool, error) {
	if ss.BloomFilter != nil && !ss.BloomFilter.In([]byte(key)) {
		return nil, false, nil
	}

	offset, maxOffset := ss.Index.GetOffsets(key)
	if maxOffset < 0 {
		maxOffset = ss.FileSize
	}

	chunkB := *sm.bytesPool.Get().(*[]byte)
	if cap(chunkB) < maxOffset-offset {
		chunkB = make([]byte, maxOffset-offset)
	}
	chunkB = chunkB[:maxOffset-offset]
	defer sm.bytesPool.Put(&chunkB)

	chunk, err := readChunkWithBuffer(ss.DataFile, offset, chunkB)
	if err != nil {
		return nil, false, fmt.Errorf("unable to read chunk: %w", err)
	}
	buf := bytes.NewBuffer(chunk)

	for buf.Len() > 0 {
		kvp, _, err := readKeyVal(buf)
		if err != nil {
			return nil, false, fmt.Errorf("unable to find in SSTable: %w", err)
		}
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

func encodeFiles(dir string, id int, meta *Meta, si *SparseIndex, bf *bloom.BloomFilterV2) error {
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
