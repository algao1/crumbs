package main

import (
	"crumbs/dbs/lsm"
	"fmt"
	"os"
	"time"
)

func main() {
	cleanUp()

	lt, err := lsm.NewLSMTree("cmd/data")
	if err != nil {
		panic(err)
	}

	for i := 0; i < 500000; i++ {
		key := fmt.Sprintf("key_%d", i)
		val := []byte(fmt.Sprintf("val_%d", i))
		lt.Put(key, val)
	}
	lt.Close()

	t := time.Now()
	for i := 0; i < 500000; i++ {
		key := fmt.Sprintf("key_%d", i)
		val := fmt.Sprintf("val_%d", i)
		b, _ := lt.Get(key)
		if string(b) != val {
			fmt.Println(string(b), val)
			panic("oops")
		}
	}
	fmt.Println("search1 finished in", time.Since(t))
	lt.Compact()

	_, err = lsm.NewLSMTree("cmd/data")
	if err != nil {
		panic(err)
	}

	t = time.Now()
	for i := 0; i < 500000; i++ {
		key := fmt.Sprintf("key_%d", i)
		val := fmt.Sprintf("val_%d", i)
		b, _ := lt.Get(key)
		if string(b) != val {
			fmt.Println(string(b), val)
			panic("oops")
		}
	}
	fmt.Println("search2 finished in", time.Since(t))
}

func cleanUp() {
	os.RemoveAll("cmd/data")
}
