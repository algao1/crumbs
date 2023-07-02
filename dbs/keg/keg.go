package keg

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Keg struct {
	mu sync.RWMutex

	dir     string
	keyDir  map[string]Hint
	bufPool sync.Pool

	active ActiveFile
	stale  map[uint32]StaleFile
}

func New(dir string) (*Keg, error) {
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return nil, fmt.Errorf("unable to initialize directory: %w", err)
	}

	k := &Keg{
		dir:    dir,
		keyDir: make(map[string]Hint),
		bufPool: sync.Pool{New: func() any {
			return bytes.NewBuffer([]byte{})
		}},
		stale: make(map[uint32]StaleFile),
	}

	if err := k.loadActiveFile(); err != nil {
		return nil, fmt.Errorf("unable to load active file: %w", err)
	}

	if err := k.loadKeyDir(); err != nil {
		return nil, fmt.Errorf("unable to load key dir from files: %w", err)
	}

	return k, nil
}

func (k *Keg) Put(key, value []byte) error {
	k.mu.Lock()
	defer k.mu.Unlock()

	if err := k.put(key, value); err != nil {
		return err
	}
	k.keyDir[string(key)] = Hint{
		FileID:      k.active.FileID,
		ValueOffset: k.active.Offset - uint32(len(value)+len(key)),
		ValueSize:   uint32(len(value)),
	}
	return nil
}

func (k *Keg) Get(key []byte) ([]byte, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()

	hint, ok := k.keyDir[string(key)]
	if !ok {
		return []byte{}, nil
	}

	reader := k.active.Reader
	if hint.FileID != k.active.FileID {
		reader = k.stale[hint.FileID].Reader
	}

	v := make([]byte, hint.ValueSize)
	_, err := reader.ReadAt(v, int64(hint.ValueOffset))
	if err != nil {
		return nil, fmt.Errorf("unable to read value for get %s: %w", key, err)
	}
	return v, nil
}

func (k *Keg) Fold(f func(k string, v []byte)) error {
	k.mu.RLock()
	for key, hint := range k.keyDir {
		var reader io.ReaderAt
		if hint.FileID != k.active.FileID {
			reader = k.stale[hint.FileID].Reader
		}
		v := make([]byte, hint.ValueOffset)
		_, err := reader.ReadAt(v, int64(hint.ValueOffset))
		if err != nil {
			return fmt.Errorf("unable to read value for fold: %w", err)
		}
		f(key, v)
	}
	k.mu.RUnlock()
	return nil
}

func (k *Keg) Delete(key []byte) (uint32, error) {
	k.mu.RLock()
	h, ok := k.keyDir[string(key)]
	k.mu.RUnlock()

	if !ok {
		return 0, nil
	}

	err := k.Put(key, []byte{})
	if err != nil {
		return 0, fmt.Errorf("unable to delete key: %w", err)
	}

	k.mu.Lock()
	delete(k.keyDir, string(key))
	k.mu.Unlock()

	return h.ValueSize + HEADER_SIZE, nil
}

func (k *Keg) Close() {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.active.Writer.Close()
}

// TODO: I don't love the design of Compact(), but will
// leave for another day to change.

func (k *Keg) Compact() error {
	staleKeys, staleFileIDs := k.getStaleKeysFileIDs()
	if len(staleFileIDs) == 0 {
		return nil
	}

	tempKeg, err := New(filepath.Join(k.dir, "temp"))
	if err != nil {
		return fmt.Errorf("unable to create temp keg: %w", err)
	}

	for _, sk := range staleKeys {
		v, err := k.Get(sk)
		if err != nil {
			// If key doesn't exist, or error, doesn't matter.
			continue
		}
		err = tempKeg.Put(sk, v)
		if err != nil {
			return fmt.Errorf("unable to put in temp keg: %w", err)
		}
	}
	tempKeg.Close()
	numFiles := int(tempKeg.active.FileID) + 1

	k.mu.Lock()
	// Rotate files to make space for temp files.
	baseFileID := k.active.FileID
	k.rotate(uint32(numFiles + 1))
	k.mu.Unlock()

	err = k.moveTempFiles(tempKeg.dir, baseFileID)
	if err != nil {
		return fmt.Errorf("unable to move temp files: %w", err)
	}

	fileKeyDir := make(map[uint32]map[string]Hint)
	k.mu.Lock()
	for i := 0; i < numFiles; i++ {
		fID := baseFileID + uint32(i+1)
		fName := kegFile(k.dir, fID)
		fileKeyDir[fID] = make(map[string]Hint)

		f, err := os.Open(fName)
		if err != nil {
			return fmt.Errorf("unable to open file %s: %w", fName, err)
		}
		k.stale[fID] = StaleFile{Reader: f, FileID: fID}
	}
	k.mu.Unlock()

	// Update keydir. We move the temporary files as a contiguous collection.
	for nk, h := range tempKeg.keyDir {
		k.mu.Lock()
		if _, ok := k.keyDir[nk]; ok {
			h.FileID += baseFileID + 1
			k.keyDir[nk] = h
			fileKeyDir[h.FileID][nk] = h
		} else {
			delete(tempKeg.keyDir, nk)
		}
		k.mu.Unlock()
	}

	err = k.removeStaleFiles(staleFileIDs)
	if err != nil {
		return fmt.Errorf("unable to remove stale files: %w", err)
	}
	err = k.generateHintFiles(fileKeyDir, numFiles, baseFileID)
	if err != nil {
		return fmt.Errorf("unable to generate hint files: %w", err)
	}

	return nil
}

func (k *Keg) put(key, value []byte) error {
	header := Header{
		Timestamp: uint32(time.Now().Unix()),
		KeySize:   uint32(len(key)),
		ValueSize: uint32(len(value)),
	}

	buf := k.bufPool.Get().(*bytes.Buffer)
	defer k.bufPool.Put(buf)
	defer buf.Reset()

	if err := header.encode(buf); err != nil {
		return fmt.Errorf("unable to encode header: %w", err)
	}
	if _, err := buf.Write(value); err != nil {
		return fmt.Errorf("unable to write value: %w", err)
	}
	if _, err := buf.Write(key); err != nil {
		return fmt.Errorf("unable to write key: %w", err)
	}

	if k.active.Offset+uint32(buf.Len()) > MAX_FILE_SIZE {
		err := k.rotate(1)
		if err != nil {
			return fmt.Errorf("unable to rotate file: %w", err)
		}
	}

	n, err := k.active.Writer.Write(buf.Bytes())
	if err != nil {
		return fmt.Errorf("unable to write buffer to file: %w", err)
	}
	k.active.Offset += uint32(n)

	return nil
}

func (k *Keg) getStaleKeysFileIDs() ([][]byte, map[uint32]any) {
	staleKeys := make([][]byte, 0)
	staleFileIDs := make(map[uint32]any)

	k.mu.RLock()
	for key, hint := range k.keyDir {
		if hint.FileID != k.active.FileID {
			staleKeys = append(staleKeys, []byte(key))
			staleFileIDs[hint.FileID] = struct{}{}
		}
	}
	k.mu.RUnlock()

	if len(staleKeys) == 0 {
		k.mu.RLock()
		for i := uint32(0); i < k.active.FileID; i++ {
			staleFileIDs[i] = struct{}{}
		}
		k.mu.RUnlock()
	}

	return staleKeys, staleFileIDs
}

func (k *Keg) moveTempFiles(tempDir string, baseFileID uint32) error {
	tempFiles, err := filepath.Glob(fmt.Sprintf("%s/*.keg", tempDir))
	if err != nil {
		return fmt.Errorf("unable to glob temp files: %w", err)
	}

	for idx, file := range tempFiles {
		newFileName := kegFile(k.dir, baseFileID+uint32(idx+1))
		err := os.Rename(file, newFileName)
		if err != nil {
			return fmt.Errorf("unable to rename temp file: %w", err)
		}
	}

	return nil
}

func (k *Keg) removeStaleFiles(fileIDs map[uint32]any) error {
	for id := range fileIDs {
		file := kegFile(k.dir, id)
		if err := os.Remove(file); err != nil {
			return fmt.Errorf("unable to remove file: %w", err)
		}
		k.mu.Lock()
		delete(k.stale, id)
		k.mu.Unlock()
	}
	return nil
}

func (k *Keg) generateHintFiles(fileKeyDir map[uint32]map[string]Hint, n int, baseFileID uint32) error {
	for i := 0; i < n; i++ {
		fName := hintFile(k.dir, baseFileID+uint32(i+1))
		fKeyDir := fileKeyDir[baseFileID+uint32(i+1)]

		f, err := os.OpenFile(fName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return fmt.Errorf("unable to open file: %w", err)
		}

		err = gob.NewEncoder(bufio.NewWriter(f)).Encode(fKeyDir)
		if err != nil {
			return fmt.Errorf("unable to encode keyDir: %w", err)
		}
	}
	return nil
}

// rotate closes the current file and opens a new one.
// Assumes that the caller has acquired the lock.
func (k *Keg) rotate(incr uint32) error {
	if err := k.active.Writer.Close(); err != nil {
		return fmt.Errorf("unable to close file: %w", err)
	}

	k.stale[k.active.FileID] = StaleFile{Reader: k.active.Reader, FileID: k.active.FileID}
	k.active.FileID += incr
	k.active.Offset = 0

	fpath := kegFile(k.dir, k.active.FileID)

	writer, err := os.OpenFile(fpath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("unable to open file: %w", err)
	}
	reader, err := os.Open(fpath)
	if err != nil {
		return fmt.Errorf("unable to open file: %w", err)
	}

	k.active.Writer = writer
	k.active.Reader = reader

	return nil
}

func getDataFiles(dir string) ([]string, error) {
	files, err := filepath.Glob(filepath.Join(dir, "*.keg"))
	if err != nil {
		return nil, fmt.Errorf("error globbing data files: %s", err)
	}
	return files, nil
}

func getIDFromFile(file string) (uint32, error) {
	var id uint32
	_, err := fmt.Sscanf(filepath.Base(file), "%d.keg", &id)
	if err != nil {
		return 0, fmt.Errorf("error getting file id: %s", err)
	}
	return id, nil
}

func (k *Keg) loadActiveFile() error {
	dataFiles, err := getDataFiles(k.dir)
	if err != nil {
		return fmt.Errorf("unable to get data files: %w", err)
	}

	fileID := uint32(0)
	if len(dataFiles) > 0 {
		fileID, err = getIDFromFile(dataFiles[len(dataFiles)-1])
		if err != nil {
			return fmt.Errorf("unable to get file id: %w", err)
		}
	}

	kegPath := kegFile(k.dir, fileID)

	writer, err := os.OpenFile(kegPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("unable to open file %s for write: %w", kegPath, err)
	}
	reader, err := os.Open(kegPath)
	if err != nil {
		return fmt.Errorf("unable to open file %s for read: %w", kegPath, err)
	}
	fs, err := writer.Stat()
	if err != nil {
		return fmt.Errorf("unable to stat file %s: %w", kegPath, err)
	}

	k.active = ActiveFile{
		Writer: writer,
		Reader: reader,
		FileID: fileID,
		Offset: uint32(fs.Size()),
	}
	return nil
}

// loadKeyDirFromFiles populates the keyDir.
func (k *Keg) loadKeyDir() error {
	dataFiles, err := getDataFiles(k.dir)
	if err != nil {
		return fmt.Errorf("unable to get data files: %w", err)
	}

	for i, df := range dataFiles {
		id, err := getIDFromFile(df)
		if err != nil {
			return fmt.Errorf("unable to get file id: %w", err)
		}

		file, err := os.Open(df)
		if err != nil {
			return fmt.Errorf("unable to open file: %w", err)
		}

		if i != len(dataFiles)-1 {
			k.stale[id] = StaleFile{Reader: file, FileID: id}
		}

		hintFileName := hintFile(k.dir, id)
		hintFile, err := os.Open(hintFileName)
		if err == nil {
			err = k.decodeKeyDirFromHint(hintFile)
			if err != nil {
				return err
			}
			continue
		}

		sfs, err := file.Stat()
		if err != nil {
			return fmt.Errorf("unable to stat file: %w", err)
		}

		err = k.populateKeyDirFromData(file, id, uint32(sfs.Size()))
		if err != nil {
			return fmt.Errorf("unable to populate keys: %w", err)
		}
	}

	return nil
}

// decodeKeyDirFromHint populates keyDir from the hint file.
func (k *Keg) decodeKeyDirFromHint(file *os.File) error {
	decoder := gob.NewDecoder(bufio.NewReader(file))
	var hintKeyDir map[string]Hint

	err := decoder.Decode(&hintKeyDir)
	if err != nil {
		return fmt.Errorf("unable to populate from hint file: %w", err)
	}
	for key, h := range hintKeyDir {
		k.keyDir[key] = h
	}
	return nil
}

// populateKeys populates keyDir from the data file.
func (k *Keg) populateKeyDirFromData(reader io.ReaderAt, fileID, size uint32) error {
	offset := uint32(0)

	for offset < size {
		r, err := readRecord(reader, offset)
		if err != nil {
			return fmt.Errorf("%d unable to read record: %w", offset, err)
		}

		size := HEADER_SIZE + uint32(len(r.Value)) + uint32(len(r.Key))
		if r.Header.ValueSize > 0 {
			hint := Hint{
				FileID:      fileID,
				ValueOffset: offset + HEADER_SIZE,
				ValueSize:   uint32(len(r.Value)),
			}
			k.keyDir[string(r.Key)] = hint
		}
		if r.Header.ValueSize == 0 {
			delete(k.keyDir, string(r.Key))
		}
		offset += size
	}

	return nil
}

func kegFile(dir string, fileID uint32) string {
	return fmt.Sprintf("%s/%d.keg", dir, fileID)
}

func hintFile(dir string, fileID uint32) string {
	return fmt.Sprintf("%s/%d.hint", dir, fileID)
}
