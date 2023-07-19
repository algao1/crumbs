package lsm

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
)

type SSTManager struct {
	mu sync.RWMutex

	dir       string
	ssTables  [][]SSTable
	ssCounter int

	// options.
	sparseness int
	errorPct   float64
}

type SSTMOptions struct {
	sparseness int
	errorPct   float64
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
	dataPath := filepath.Join(sm.dir, fmt.Sprintf("lsm-%d.data", sm.ssCounter))
	indexPath := filepath.Join(sm.dir, fmt.Sprintf("lsm-%d.index", sm.ssCounter))
	bloomPath := filepath.Join(sm.dir, fmt.Sprintf("lsm-%d.bloom", sm.ssCounter))
	sm.ssCounter++
	sm.mu.Unlock()

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

	dataFile, err = os.Open(dataPath)
	if err != nil {
		return fmt.Errorf("unable to open data file: %w", err)
	}

	sm.mu.Lock()
	sm.ssTables[0] = append(sm.ssTables[0], SSTable{
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

		sm.ssTables[0] = append(sm.ssTables[0], SSTable{
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
	sm.ssCounter = ssCounter + 1

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

type ssFiles struct {
	dataFiles  []string
	indexFiles []string
	bloomFiles []string
}

func getFiles(dir string) (ssFiles, error) {
	dataFiles, err := filepath.Glob(filepath.Join(dir, "lsm-*.data"))
	if err != nil {
		return ssFiles{}, fmt.Errorf("unable to glob data files: %w", err)
	}

	indexFiles, err := filepath.Glob(filepath.Join(dir, "lsm-*.index"))
	if err != nil {
		return ssFiles{}, fmt.Errorf("unable to glob index files: %w", err)
	}

	bloomFiles, err := filepath.Glob(filepath.Join(dir, "lsm-*.bloom"))
	if err != nil {
		return ssFiles{}, fmt.Errorf("unable to glob bloom files: %w", err)
	}

	return ssFiles{
		dataFiles:  dataFiles,
		indexFiles: indexFiles,
		bloomFiles: bloomFiles,
	}, nil
}
