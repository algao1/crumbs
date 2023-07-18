package main

import (
	"crumbs/dbs/keg"
	"crumbs/dbs/lsm"
	"os"
)

type LsmWrapper struct {
	lt  *lsm.LSMTree
	dir string
}

func NewLSMWrapper(dir string) *LsmWrapper {
	lt, err := lsm.NewLSMTree(dir)
	if err != nil {
		panic(err)
	}
	return &LsmWrapper{
		lt:  lt,
		dir: dir,
	}
}

func (lw *LsmWrapper) Put(key string, val []byte) error {
	lw.lt.Put(key, val)
	return nil
}

func (lw *LsmWrapper) Get(key string) ([]byte, error) {
	return lw.lt.Get(key)
}

func (lw *LsmWrapper) Delete(key string) error {
	lw.lt.Delete(key)
	return nil
}

func (lw *LsmWrapper) Close() {
	lw.lt.Close()
}

func (lw *LsmWrapper) Reset() {
	os.RemoveAll(lw.dir)
	lw.lt, _ = lsm.NewLSMTree(lw.dir)
}

type KegWrapper struct {
	kdb *keg.Keg
	dir string
}

func NewKegWrapper(dir string) *KegWrapper {
	kdb, _ := keg.New(dir)
	return &KegWrapper{
		kdb: kdb,
		dir: dir,
	}
}

func (kdb *KegWrapper) Put(key string, val []byte) error {
	return kdb.kdb.Put([]byte(key), val)
}

func (kdb *KegWrapper) Get(key string) ([]byte, error) {
	return kdb.kdb.Get([]byte(key))
}

func (kdb *KegWrapper) Delete(key string) error {
	_, err := kdb.kdb.Delete([]byte(key))
	return err
}

func (kdb *KegWrapper) Close() {
	kdb.kdb.Close()
}

func (kdb *KegWrapper) Reset() {
	os.RemoveAll(kdb.dir)
	kdb.kdb, _ = keg.New(kdb.dir)
}
