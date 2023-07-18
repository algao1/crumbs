package main

import (
	"fmt"
	"math/rand"
	"os"
	"time"
)

func main() {
	// f, err := os.Create("cpu_profile.pprof")
	// if err != nil {
	// 	fmt.Println("unable to create CPU profile: ", err)
	// 	return
	// }
	// defer f.Close()

	// if err := pprof.StartCPUProfile(f); err != nil {
	// 	fmt.Println("unable to start CPU profile: ", err)
	// 	return
	// }
	// defer pprof.StopCPUProfile()

	cleanUp()

	dbs := []struct {
		name  string
		dir   string
		store KVStore
	}{
		{
			name:  "Keg",
			dir:   "data/keg",
			store: NewKegWrapper("data/keg"),
		},
		{
			name:  "LSM",
			dir:   "data/lsm",
			store: NewLSMWrapper("data/lsm"),
		},
	}

	numOps := 1000000 / 3

	for _, db := range dbs {
		fmt.Printf("Benchmarks for %s\n", db.name)
		benchPutKeyVals(db.store, numOps, 16)
		db.store.Close()
		benchSeqGetKeyVals(db.store, numOps, 16)
		benchRandGetKeyVals(db.store, numOps)

		fmt.Println()
	}
}

type KVStore interface {
	Put(key string, val []byte) error
	Get(key string) ([]byte, error)
	Delete(key string) error
	Close()
	Reset()
}

func benchPutKeyVals(store KVStore, numOps, strSize int) {
	t := time.Now()
	for i := 0; i < numOps; i++ {
		val := []byte(fmt.Sprintf("val_%d", i))
		err := store.Put(fmt.Sprintf("key_%d", i), val)
		if err != nil {
			panic(err)
		}
	}
	fmt.Printf("\tbenchPutKeyVals: %s\n", time.Since(t))
	fmt.Printf("\t\t%.0f ops/s\n", float64(numOps)/time.Since(t).Seconds())
}

func benchSeqGetKeyVals(store KVStore, numOps, strSize int) {
	t := time.Now()
	for i := 0; i < numOps; i++ {
		_, err := store.Get(fmt.Sprintf("key_%d", i))
		if err != nil {
			panic(err)
		}
	}
	fmt.Printf("\tbenchSeqGetKeyVals: %s\n", time.Since(t))
	fmt.Printf("\t\t%.0f ops/s\n", float64(numOps)/time.Since(t).Seconds())
}

func benchRandGetKeyVals(store KVStore, numOps int) {
	t := time.Now()
	for i := 0; i < numOps; i++ {
		idx := rand.Intn(numOps)
		_, err := store.Get(fmt.Sprintf("key_%d", idx))
		if err != nil {
			panic(err)
		}
	}
	fmt.Printf("\tbenchRandGetKeyVals: %s\n", time.Since(t))
	fmt.Printf("\t\t%.0f ops/s\n", float64(numOps)/time.Since(t).Seconds())
}

func cleanUp() {
	os.RemoveAll("data")
}
