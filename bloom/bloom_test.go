package bloom

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/rand"
)

func BenchmarkBloomFilterV1(b *testing.B) {
	bf, _ := NewBloomFilterV1(10_000, 0.01)
	benchmarkFilter(b, bf)
}

func BenchmarkBloomFilterV2(b *testing.B) {
	bf, _ := NewBloomFilterV2(10_000, 0.01)
	benchmarkFilter(b, bf)
}

func TestBloomFilterV1(t *testing.T) {
	f := func(n int, fpr float64) (filter, error) {
		return NewBloomFilterV1(n, fpr)
	}
	testFilter(t, f, 0.15)
}

func TestBloomFilterV2(t *testing.T) {
	f := func(n int, fpr float64) (filter, error) {
		return NewBloomFilterV2(n, fpr)
	}
	testFilter(t, f, 0.01)
}

type createFilter func(n int, fpr float64) (filter, error)

type filter interface {
	Add([]byte)
	In([]byte) bool
	Encode(string) error
	Decode(string) error
}

func benchmarkFilter(b *testing.B, filter filter) {
	bytes := make([]byte, 256)
	for range 10_000 {
		rand.Read(bytes)
		filter.Add(bytes)
		filter.In(bytes)
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		rand.Read(bytes)
		filter.Add(bytes)
	}
}

func testFilter(t *testing.T, f createFilter, tolerance float64) {
	filter, err := f(10_000, 0.01)
	assert.NoError(t, err)

	seen := make(map[string]struct{})
	bytes := make([]byte, 256)
	for range 10_000 {
		rand.Read(bytes)
		filter.Add(bytes)
		require.True(t, filter.In(bytes))
		seen[string(bytes)] = struct{}{}
	}

	filter.Encode(".test")
	defer os.RemoveAll(".test")
	filter, _ = f(10_000, 0.01)
	filter.Decode(".test")

	for key := range seen {
		require.True(t, filter.In([]byte(key)))
	}

	var total int
	var fp int

	for range 100_000 {
		total++
		rand.Read(bytes)
		if filter.In(bytes) {
			fp++
		}
	}
	assert.InDelta(t, 0.01, float64(fp)/float64(total), tolerance)
}
