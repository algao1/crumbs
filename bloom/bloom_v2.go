package bloom

import (
	"encoding/gob"
	"fmt"
	"os"

	"github.com/cespare/xxhash"
)

type BloomFilterV2 struct {
	Bitset []bool
	K      int
}

func NewBloomFilterV2(n int, fpr float64) (*BloomFilterV2, error) {
	k, m := optimalKM(float64(n), fpr)
	if k < 0 || m < 0 {
		return nil, fmt.Errorf("unable to select k and m (n=%d, fpr=%.3f)", n, fpr)
	}

	bf := BloomFilterV2{
		Bitset: make([]bool, m),
		K:      k,
	}
	return &bf, nil
}

func (bf *BloomFilterV2) Add(b []byte) {
	h := xxhash.Sum64(b)
	for i := range bf.K {
		pos := cheatHash(h, i) % uint32(len(bf.Bitset))
		bf.Bitset[pos] = true
	}
}

func (bf *BloomFilterV2) In(b []byte) bool {
	h := xxhash.Sum64(b)
	for i := range bf.K {
		pos := cheatHash(h, i) % uint32(len(bf.Bitset))
		if !bf.Bitset[pos] {
			return false
		}
	}
	return true
}

func (bf *BloomFilterV2) Encode(filename string) error {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("unable to open file for bloom filter: %w", err)
	}
	defer file.Close()

	err = gob.NewEncoder(file).Encode(bf)
	if err != nil {
		return fmt.Errorf("unable to encode bloom filter: %w", err)
	}
	return nil
}

func (bf *BloomFilterV2) Decode(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("unable to open file for bloom filter: %w", err)
	}
	defer file.Close()

	var nbf BloomFilterV2
	err = gob.NewDecoder(file).Decode(&nbf)
	if err != nil {
		return fmt.Errorf("unable to decode bloom filter: %w", err)
	}
	*bf = nbf

	return nil
}

func cheatHash(h uint64, i int) uint32 {
	return uint32(h) + uint32(i)*uint32(h>>32)
}
