package main

import (
	"crumbs/dbs/lsm"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime/pprof"
	"time"
)

const TESTDIR = ".test"

var cpuProfile bool

func init() {
	flag.BoolVar(&cpuProfile, "cpuprofile", false, "enable cpu profiling")
	flag.Parse()
}

func main() {
	os.RemoveAll(TESTDIR)
	db, err := lsm.NewLSMTree(
		TESTDIR,
		lsm.WithSparseness(8),
		lsm.WithMemTableSize(1024*1024*4),
		lsm.WithFlushPeriod(10*time.Second),
	)
	if err != nil {
		panic(err)
	}

	if cpuProfile {
		f, err := os.Create("cpu.pprof")
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	t := time.Now()
	b := make([]byte, 128)
	for i := range 1_000_000 {
		db.Put(fmt.Sprintf("key_%d", i), b)
	}
	fmt.Println("writing 1,000,000 entries", time.Since(t))

	db.Close()

	t = time.Now()
	for range 250_000 {
		db.Get(fmt.Sprintf("key_%d", rand.Intn(1_000_000)))
	}
	fmt.Println("reading 250,000 entries (uncompacted)", time.Since(t))

	t = time.Now()
	db.Compact()
	fmt.Println("compacting", time.Since(t))

	t = time.Now()
	for range 250_000 {
		db.Get(fmt.Sprintf("key_%d", rand.Intn(1_000_000)))
	}
	fmt.Println("reading 250,000 entries", time.Since(t))
}
