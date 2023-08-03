package main

import (
	"crumbs/dbs/lsm"
	"fmt"
	"os"
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
	for i := 0; i < 500000; i++ {
		key := fmt.Sprintf("key_%d", i)
		val := fmt.Sprintf("val_%d", i)
		b, _ := lt.Get(key)
		if string(b) != val {
			panic("oops")
		}
	}
	lt.Close()

	_, err = lsm.NewLSMTree("cmd/data")
	if err != nil {
		panic(err)
	}
}

func cleanUp() {
	os.RemoveAll("cmd/data")
}
