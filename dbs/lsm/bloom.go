package lsm

import (
	"encoding/gob"
	"fmt"
	"math"
	"os"
	"strings"

	"github.com/cespare/xxhash/v2"
)

type hashFunc func([]byte) int

type BloomFilter struct {
	Bitset    []bool
	hashFuncs []hashFunc
	K         int
}

func NewBloomFilter(n int, dfp float64) (*BloomFilter, error) {
	k, m := optimalKM(float64(n), dfp)
	if k < 0 || m < 0 {
		return nil, fmt.Errorf("unable to select k and m")
	}

	bf := BloomFilter{
		Bitset:    make([]bool, m),
		K:         k,
		hashFuncs: make([]hashFunc, k),
	}

	for i := 0; i < k; i++ {
		pad := []byte(strings.Repeat(".", i))
		bf.hashFuncs[i] = func(b []byte) int {
			b = append(b, pad...)
			ret := int(xxhash.Sum64(b))
			if ret < 0 {
				ret *= -1
			}
			return ret
		}
	}

	return &bf, nil
}

func (bf *BloomFilter) Add(k []byte) {
	m := len(bf.Bitset)
	for _, hf := range bf.hashFuncs {
		idx := hf(k) % m
		bf.Bitset[idx] = true
	}
}

func (bf *BloomFilter) In(k []byte) bool {
	m := len(bf.Bitset)
	for _, hf := range bf.hashFuncs {
		idx := hf(k) % m
		if !bf.Bitset[idx] {
			return false
		}
	}
	return true
}

func (bf *BloomFilter) Encode(filename string) error {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("unable to open file for bloom filter: %w", err)
	}
	defer file.Close()

	err = gob.NewEncoder(file).Encode(BloomFilter{
		Bitset: bf.Bitset,
		K:      bf.K,
	})
	if err != nil {
		return fmt.Errorf("unable to encode bloom filter: %w", err)
	}
	return nil
}

func (bf *BloomFilter) Decode(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("unable to open file for bloom filter: %w", err)
	}
	defer file.Close()

	var nbf BloomFilter
	err = gob.NewDecoder(file).Decode(&nbf)
	if err != nil {
		return fmt.Errorf("unable to decode bloom filter: %w", err)
	}
	nbf.hashFuncs = make([]hashFunc, nbf.K)

	for i := 0; i < nbf.K; i++ {
		pad := []byte(strings.Repeat(".", i))
		nbf.hashFuncs[i] = func(b []byte) int {
			b = append(b, pad...)
			ret := int(xxhash.Sum64(b))
			if ret < 0 {
				ret *= -1
			}
			return ret
		}
	}

	*bf = nbf
	return nil
}

func optimalKM(n, dfp float64) (int, int) {
	var m, k float64
	l, r := float64(0), 100*n

	for l < r {
		m = l + (r-l)/2
		k = (m / n) * math.Log(2)
		fp := math.Pow((1 - math.Exp(-k*n/m)), k)
		if math.Abs(dfp-fp) < dfp/10 {
			return int(math.Round(k)), int(math.Round(m))
		}

		if fp < dfp {
			r = m
		} else {
			l = m + 1
		}
	}

	return -1, -1
}
