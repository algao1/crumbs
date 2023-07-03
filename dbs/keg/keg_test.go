package keg

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

const TEST_DIR = ".testdata"

func init() {
	cleanupKeg()
}

func initKeg() *Keg {
	k, err := New(TEST_DIR)
	if err != nil {
		panic(err)
	}
	return k
}

func cleanupKeg() {
	os.RemoveAll(TEST_DIR)
}

func TestPutGet(t *testing.T) {
	k := initKeg()

	var tests = []struct {
		key, val string
	}{
		{"key", "val"},
		{"user", "password"},
		{"cheese", "cake"},
		{"chocolate", "milk"},
	}

	for _, tt := range tests {
		err := k.Put([]byte(tt.key), []byte(tt.val))
		assert.Nil(t, err)
		v, err := k.Get([]byte(tt.key))
		assert.Nil(t, err)
		assert.Equal(t, tt.val, string(v))
	}

	t.Cleanup(func() {
		cleanupKeg()
	})
}

func TestDelete(t *testing.T) {
	k := initKeg()

	var tests = []struct {
		key string
		val string
	}{
		{"key", "val"},
		{"user", "password"},
		{"cheese", "cake"},
		{"_chocolate", "milk"},
		{"_strawberry", "milk"},
	}

	for _, tt := range tests {
		expectDeleted := uint32(0)
		if tt.key[0] != '_' {
			expectDeleted = uint32(len(tt.val)) + HEADER_SIZE
			k.Put([]byte(tt.key), []byte(tt.val))
		}

		n, err := k.Delete([]byte(tt.key))
		assert.Nil(t, err)
		assert.Equal(t, expectDeleted, n)
	}

	t.Cleanup(func() {
		cleanupKeg()
	})
}

func TestFold(t *testing.T) {
	size := 1000

	k := initKeg()

	for i := 0; i < size; i++ {
		err := k.Put(
			[]byte(fmt.Sprintf("key_%d", i)),
			[]byte(fmt.Sprintf("val_%d", i)),
		)
		assert.Nil(t, err)
	}

	unique := make(map[string]any)
	err := k.Fold(func(k, v []byte) {
		unique[string(k)] = struct{}{}
	})
	assert.Nil(t, err)
	assert.Equal(t, size, len(unique))

	t.Cleanup(func() {
		cleanupKeg()
	})
}

func TestPutRotate(t *testing.T) {
	rotateAfter := 100
	size := 1000

	k := initKeg()
	for i := 0; i < size; i++ {
		if i%rotateAfter == 0 {
			k.rotate(1)
		}
		err := k.Put(
			[]byte(fmt.Sprintf("key_%d", i)),
			[]byte(fmt.Sprintf("val_%d", i)),
		)
		assert.Nil(t, err)
	}

	k = initKeg()
	for i := 0; i < size; i++ {
		v, err := k.Get([]byte(fmt.Sprintf("key_%d", i)))
		assert.Nil(t, err)
		assert.Equal(t, fmt.Sprintf("val_%d", i), string(v))
	}

	t.Cleanup(func() {
		cleanupKeg()
	})
}

func TestDeleteCompact(t *testing.T) {
	rotateAfter := 100
	size := 1000

	k := initKeg()

	for i := 0; i < size; i++ {
		if i%rotateAfter == 0 {
			k.rotate(1)
		}
		err := k.Put(
			[]byte(fmt.Sprintf("key_%d", i)),
			[]byte(fmt.Sprintf("val_%d", i)),
		)
		assert.Nil(t, err)
	}

	for i := 0; i < size; i += 3 {
		_, err := k.Delete(
			[]byte(fmt.Sprintf("key_%d", i)),
		)
		assert.Nil(t, err)
	}
	assert.Nil(t, k.Compact())

	k = initKeg()
	for i := 0; i < size; i++ {
		expectV := []byte(fmt.Sprintf("val_%d", i))
		if i%3 == 0 {
			expectV = []byte{}
		}
		v, err := k.Get([]byte(fmt.Sprintf("key_%d", i)))
		assert.Nil(t, err)
		assert.Equal(t, expectV, v)
	}

	t.Cleanup(func() {
		cleanupKeg()
	})
}
