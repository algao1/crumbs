package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"runtime/pprof"
	"sync"
	"time"
)

var (
	profile bool
)

func init() {
	flag.BoolVar(&profile, "profile", false, "profile cpu")
	flag.Parse()
}

func main() {
	if profile {
		f, err := os.Create("cpu_profile.pprof")
		if err != nil {
			fmt.Println("unable to create CPU profile: ", err)
			return
		}
		defer f.Close()

		if err := pprof.StartCPUProfile(f); err != nil {
			fmt.Println("unable to start CPU profile: ", err)
			return
		}
		defer pprof.StopCPUProfile()
	}

	defer cleanUp()
	cleanUp()

	dbs := []struct {
		name  string
		dir   string
		store kvStore
	}{
		// {
		// 	name:  "Keg",
		// 	dir:   "data/keg",
		// 	store: NewKegWrapper("data/keg"),
		// },
		{
			name:  "LSM",
			dir:   "data/lsm",
			store: NewLSMWrapper("data/lsm"),
		},
	}

	numOps := 1000000

	for _, db := range dbs {
		fmt.Printf("Benchmarks for %s\n", db.name)
		benchPutKeyVals(db.store, numOps, 16)
		db.store.Close()
		benchSeqGetKeyVals(db.store, numOps, 16)
		benchRandGetKeyVals(db.store, numOps)
		benchConcRandGetKeyVals(db.store, numOps)

		fmt.Println()
	}
}

type kvStore interface {
	Put(key string, val []byte) error
	Get(key string) ([]byte, error)
	Delete(key string) error
	Close()
	Reset()
}

func printBenchmarkResults(name string, elapsed time.Duration, numOps int) {
	fmt.Printf("%-20s%14s%12d ops%12.0f ops/s\n", name, elapsed, numOps, float64(numOps)/elapsed.Seconds())
}

func benchPutKeyVals(store kvStore, numOps, strSize int) {
	t := time.Now()
	for i := 0; i < numOps; i++ {
		val := []byte(fmt.Sprintf("val_%d", i))
		err := store.Put(fmt.Sprintf("key_%d", i), val)
		if err != nil {
			panic(err)
		}
	}
	printBenchmarkResults("PutKeyVals", time.Since(t), numOps)
}

func benchSeqGetKeyVals(store kvStore, numOps, strSize int) {
	t := time.Now()
	for i := 0; i < numOps; i++ {
		v, err := store.Get(fmt.Sprintf("key_%d", i))
		if err != nil {
			panic(err)
		}
		if string(v) != fmt.Sprintf("val_%d", i) {
			panic(fmt.Sprintf("incorrect value found, expected: val_%d, got: %s", i, v))
		}
	}
	printBenchmarkResults("SeqGetKeyVals", time.Since(t), numOps)
}

func benchRandGetKeyVals(store kvStore, numOps int) {
	t := time.Now()
	for i := 0; i < numOps; i++ {
		idx := rand.Intn(numOps)
		_, err := store.Get(fmt.Sprintf("key_%d", idx))
		if err != nil {
			panic(err)
		}
	}
	printBenchmarkResults("RandGetKeyVals", time.Since(t), numOps)
}

func benchConcRandGetKeyVals(store kvStore, numOps int) {
	t := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < numOps; i++ {
		idx := rand.Intn(numOps)
		wg.Add(1)
		go func() {
			store.Get(fmt.Sprintf("key_%d", idx))
			wg.Done()
		}()
	}
	wg.Wait()
	printBenchmarkResults("ConcRandGetKeyVals", time.Since(t), numOps)
}

func cleanUp() {
	os.RemoveAll("data")
}
