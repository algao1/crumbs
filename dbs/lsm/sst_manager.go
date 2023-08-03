package lsm

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"sync"
)

type SSTManager struct {
	mu sync.RWMutex

	dir       string
	ssTables  [][]SSTable
	ssCounter int

	// // set to false when under compaction.
	// writeable bool
	// writeMu   sync.RWMutex

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

	Meta        Meta
	Index       *SparseIndex
	BloomFilter *BloomFilter
	DataFile    io.ReaderAt
}

func NewSSTManager(dir string, opts SSTMOptions) *SSTManager {
	sm := &SSTManager{
		dir:        dir,
		ssTables:   make([][]SSTable, 1),
		sparseness: opts.sparseness,
		errorPct:   opts.errorPct,
	}
	sm.ssTables[0] = make([]SSTable, 0)
	return sm
}

// Add adds and writes a memtable to disk as a SSTable. Requires
// that the memtable is not the active (most recent) memtable.
func (sm *SSTManager) Add(mt Memtable) error {
	sm.mu.Lock()
	curCounter := sm.ssCounter
	sm.ssCounter++
	sm.mu.Unlock()

	// TODO: generalize this.
	metaPath := filepath.Join(sm.dir, fmt.Sprintf("lsm-%d.meta", curCounter))
	dataPath := filepath.Join(sm.dir, fmt.Sprintf("lsm-%d.data", curCounter))
	indexPath := filepath.Join(sm.dir, fmt.Sprintf("lsm-%d.index", curCounter))
	bloomPath := filepath.Join(sm.dir, fmt.Sprintf("lsm-%d.bloom", curCounter))

	dataFile, err := os.OpenFile(dataPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("unable to flush: %w", err)
	}
	defer dataFile.Close()

	sparseIndex := NewSparseIndex()
	bf, err := NewBloomFilter(mt.Nodes(), sm.errorPct)
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

		if iter%sm.sparseness == 0 {
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

	meta := Meta{}
	if err := meta.Encode(metaPath); err != nil {
		return fmt.Errorf("unable to flush metadata: %w", err)
	}

	dataFile, err = os.Open(dataPath)
	if err != nil {
		return fmt.Errorf("unable to open data file: %w", err)
	}

	sm.mu.Lock()
	sm.ssTables[0] = append(sm.ssTables[0], SSTable{
		ID:          curCounter,
		FileSize:    offset,
		Index:       sparseIndex,
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
		var meta Meta
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

// findInSSTable expects caller to acquire read lock on SSTables.
func findInSSTable(ss SSTable, key string) ([]byte, bool, error) {
	if !ss.BloomFilter.In([]byte(key)) {
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
