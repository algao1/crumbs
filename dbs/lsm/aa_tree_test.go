package lsm

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/magiconair/properties/assert"
)

func TestInsertDelete(t *testing.T) {
	set1 := NewAATree()
	set2 := make(map[string][]byte)

	for range 100_000 {
		k := fmt.Sprintf("%d", rand.Intn(1_000))
		op := rand.Intn(100)

		if op < 50 {
			set1.Insert(k, nil)
			set2[k] = nil
		} else {
			set1.Remove(k)
			delete(set2, k)
		}

		assert.Equal(t, set1.Nodes(), len(set2))
	}
}
