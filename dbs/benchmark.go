package main

import (
	"crumbs/dbs/lsm"
	"flag"
	"fmt"
	"log"
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
	if cpuProfile {
		f, err := os.Create("cpu.pprof")
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	defer os.RemoveAll(TESTDIR)
	db, err := lsm.NewLSMTree(
		TESTDIR,
		lsm.WithMemTableSize(1024*1024*4),
		lsm.WithFlushPeriod(10*time.Second),
	)
	if err != nil {
		panic(err)
	}

	t := time.Now()
	for i := range 1_000_000 {
		db.Put(fmt.Sprintf("key_%d", i), []byte(fmt.Sprintf("val_%d", i)))
	}
	fmt.Println(time.Since(t))

	t = time.Now()
	db.Close()
	fmt.Println(time.Since(t))
}
