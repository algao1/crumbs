package main

import (
	"crumbs/dbs/lsm"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func cleanUp() {
	os.RemoveAll("data")
}

func main() {
	cleanUp()
	defer cleanUp()

	db, _ := lsm.NewLSMTree("data")

	writeLagCh := make(chan time.Duration)
	readLagCh := make(chan time.Duration)

	go func() {
		for {
			rInt := rand.Int31()
			key := fmt.Sprintf("key_%d", rInt)
			val := []byte(fmt.Sprintf("val_%d", rInt))

			t := time.Now()
			db.Put(key, val)
			writeLagCh <- time.Since(t)
			time.Sleep(2 * time.Nanosecond)
		}
	}()

	go func() {
		for {
			rInt := rand.Int31()
			key := fmt.Sprintf("key_%d", rInt)

			t := time.Now()
			db.Get(key)
			readLagCh <- time.Since(t)
			time.Sleep(2 * time.Nanosecond)
		}
	}()

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT,
	)

	ticker := time.NewTicker(1 * time.Second)
	compactionTicker := time.NewTicker(25 * time.Second)

	writeTotalDur := time.Duration(0)
	writeNumber := 0
	readTotalDur := time.Duration(0)
	readNumber := 0

	for {
		select {
		case elapsed := <-writeLagCh:
			writeNumber++
			writeTotalDur += elapsed
		case elapsed := <-readLagCh:
			readNumber++
			readTotalDur += elapsed
		case <-ticker.C:
			readNumber = max(readNumber, 1)
			writeNumber = max(writeNumber, 1)

			fmt.Println("write: ", writeTotalDur/time.Duration(writeNumber))
			fmt.Println("read:  ", readTotalDur/time.Duration(readNumber))

			writeTotalDur = 0
			writeNumber = 0

			readTotalDur = 0
			readNumber = 0
		case <-compactionTicker.C:
			go db.Compact()
		case <-sigc:
			return
		}
	}
}
