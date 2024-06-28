package main

import (
	"fmt"
	"math"
	"math/rand"
)

func main() {
	buckets := make([][]int, 16)
	for i := range len(buckets) {
		buckets[i] = make([]int, 0)
	}

	for i := range int(5e3) {
		op := rand.Intn(1000)
		if op < 5 {
			buckets = append(buckets, make([]int, 0))
			shuffleLoad(buckets)
		} else {
			k := jumpConsistentHash(i, len(buckets))
			buckets[k] = append(buckets[k], i)
		}
	}
}

func shuffleLoad(buckets [][]int) {
	keysMoved, keysTotal := 0, 0

	for i := range len(buckets) - 1 {
		newBucket := make([]int, 0)
		for _, v := range buckets[i] {
			k := jumpConsistentHash(v, len(buckets))
			if k == i {
				newBucket = append(newBucket, v)
			} else {
				buckets[k] = append(buckets[k], v)
				keysMoved++
			}
		}
		buckets[i] = newBucket
	}

	for _, b := range buckets {
		keysTotal += len(b)
	}
	fmt.Printf("moved %d/%d keys\n", keysMoved, keysTotal)
}

// This is an inefficient implementation of jumpConsistentHash
// from this Google paper (https://arxiv.org/pdf/1406.2294). Here,
// they replace the RNG with some constants and bitshifting.
func jumpConsistentHash(key, numBuckets int) int {
	rng := rand.New(rand.NewSource(int64(key)))
	b, j := -1, 0
	for j < int(numBuckets) {
		b = j
		r := rng.Float64()
		j = int(math.Floor((float64(b) + 1) / r))
	}
	return b
}
