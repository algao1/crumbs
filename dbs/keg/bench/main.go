package main

import (
	"os"
)

const MAX_OPS = 10000
const BIG_STRING_SIZE = 10000

func main() {
	os.RemoveAll("bench/data")
	k, err := NewKegStore("bench/data")
	if err != nil {
		panic(err)
	}
	k.BenchPutKeys()
	k.BenchGetSeqKeys()
	k.BenchGetRandKeys()
	k.BenchFoldKeys()
}
