package keg

import (
	"fmt"
	"sync"
)

type KeyDir struct {
	mu     sync.RWMutex
	FKDirs []FileKeyDir
}

type FileKeyDir struct {
	FileID uint32
	Hints  map[string]Hint
}

func NewKeyDir() KeyDir {
	return KeyDir{
		FKDirs: make([]FileKeyDir, 0),
	}
}

func (kd *KeyDir) Add(key []byte, hint Hint) error {
	kd.mu.Lock()
	defer kd.mu.Unlock()

	n := len(kd.FKDirs)
	if n == 0 {
		return fmt.Errorf("no file key dirs")
	}
	kd.FKDirs[n-1].Hints[string(key)] = hint
	return nil
}

func (kd *KeyDir) Get(key []byte) (Hint, error) {
	kd.mu.RLock()
	defer kd.mu.RUnlock()

	for _, fkd := range kd.FKDirs {
		if h, ok := fkd.Hints[string(key)]; ok {
			return h, nil
		}
	}
	return Hint{}, fmt.Errorf("key not found")
}

func (kd *KeyDir) Fold(f func(key []byte, hint Hint) error) {
	kd.mu.RLock()
	defer kd.mu.RUnlock()

	for _, fkd := range kd.FKDirs {
		for k, hint := range fkd.Hints {
			if err := f([]byte(k), hint); err != nil {
				return
			}
		}
	}
}

func (kd *KeyDir) Delete(key []byte) error {
	kd.mu.Lock()
	defer kd.mu.Unlock()

	for _, fkd := range kd.FKDirs {
		if h, ok := fkd.Hints[string(key)]; ok {
			h.ValueSize = 0
			fkd.Hints[string(key)] = h
			return nil
		}
	}
	return nil
}

func (kd *KeyDir) AddFileKeyDir(fkd FileKeyDir) {
	kd.mu.Lock()
	defer kd.mu.Unlock()
	kd.FKDirs = append(kd.FKDirs, fkd)
}

func (kd *KeyDir) DeleteFileKeyDirs(fileIDs []uint32) {
	toDelete := make(map[uint32]any)
	for _, fid := range fileIDs {
		toDelete[fid] = struct{}{}
	}

	kd.mu.Lock()
	defer kd.mu.Unlock()

	newFKDirs := make([]FileKeyDir, len(kd.FKDirs))
	for _, fkd := range kd.FKDirs {
		if _, ok := toDelete[fkd.FileID]; !ok {
			newFKDirs = append(newFKDirs, fkd)
		}
	}
	kd.FKDirs = newFKDirs
}
