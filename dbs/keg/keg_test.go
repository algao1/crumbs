package keg

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

const TEST_DIR = ".testdata"

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

func TestPutRotate(t *testing.T) {
	k := initKeg()
	for i := 0; i < 1000; i++ {
		if i%100 == 0 {
			k.rotate(1)
		}
		err := k.Put(
			[]byte(fmt.Sprintf("key_%d", i)),
			[]byte(fmt.Sprintf("val_%d", i)),
		)
		assert.Nil(t, err)
	}

	k = initKeg()
	for i := 0; i < 1000; i++ {
		v, err := k.Get([]byte(fmt.Sprintf("key_%d", i)))
		assert.Nil(t, err)
		assert.Equal(t, fmt.Sprintf("val_%d", i), string(v))
	}

	t.Cleanup(func() {
		cleanupKeg()
	})
}

func TestDeleteCompact(t *testing.T) {
	k := initKeg()

	for i := 0; i < 1000; i++ {
		if i%100 == 0 {
			k.rotate(1)
		}
		err := k.Put(
			[]byte(fmt.Sprintf("key_%d", i)),
			[]byte(fmt.Sprintf("val_%d", i)),
		)
		assert.Nil(t, err)
	}

	for i := 0; i < 1000; i += 3 {
		_, err := k.Delete(
			[]byte(fmt.Sprintf("key_%d", i)),
		)
		assert.Nil(t, err)
	}
	assert.Nil(t, k.Compact())

	k = initKeg()
	for i := 0; i < 1000; i++ {
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
