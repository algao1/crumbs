package main

import (
	"crumbs/dbs/keg"
	"fmt"
	"math/rand"
	"time"
)

type KegStore struct {
	keg *keg.Keg
}

func bigString(size int) string {
	b := make([]byte, size)
	for i := 0; i < size; i++ {
		b[i] = 'a'
	}
	return string(b)
}

func NewKegStore(dir string) (*KegStore, error) {
	t := time.Now()
	k, err := keg.New(dir)
	if err != nil {
		return nil, err
	}
	fmt.Printf("Finished loading KegDB in %s.\n", time.Since(t))

	return &KegStore{keg: k}, nil
}

func (k *KegStore) BenchPutKeys() {
	s := bigString(BIG_STRING_SIZE)
	t := time.Now()
	for i := 0; i < MAX_OPS; i++ {
		err := k.keg.Put([]byte(fmt.Sprintf("key_%d", i)), []byte(s))
		if err != nil {
			panic(err)
		}
	}
	fmt.Printf("KegBenchPutKeys: %s\n", time.Since(t))
}

func (k *KegStore) BenchGetSeqKeys() {
	t := time.Now()
	for i := 0; i < MAX_OPS; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		_, err := k.keg.Get(key)
		if err != nil {
			panic(err)
		}
	}
	fmt.Printf("KegBenchGetSeqKeys: %s\n", time.Since(t))
}

func (k *KegStore) BenchGetRandKeys() {
	t := time.Now()
	for i := 0; i < MAX_OPS; i++ {
		key := []byte(fmt.Sprintf("key_%d", rand.Int31n(MAX_OPS)))
		_, err := k.keg.Get(key)
		if err != nil {
			panic(err)
		}
	}
	fmt.Printf("KegBenchGetRandKeys: %s\n", time.Since(t))
}

func (k *KegStore) BenchFoldKeys() {
	t := time.Now()
	err := k.keg.Fold(func(k, v []byte) {})
	if err != nil {
		panic(err)
	}
	fmt.Printf("KegBenchFoldKeys: %s\n", time.Since(t))
}
