package lsm

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

const TEST_DIR = ".testdata"

func cleanUp() {
	os.RemoveAll(TEST_DIR)
}

func TestPutGetLarge(t *testing.T) {
	lt, err := NewLSMTree(TEST_DIR)
	assert.Nil(t, err)
	defer cleanUp()

	for i := 0; i < 500000; i++ {
		key := fmt.Sprintf("key_%d", i)
		val := []byte(fmt.Sprintf("val_%d", i))
		lt.Put(key, val)
	}

	for i := 0; i < 500000; i++ {
		key := fmt.Sprintf("key_%d", i)
		val := fmt.Sprintf("val_%d", i)
		found, err := lt.Get(key)
		assert.Nil(t, err)
		assert.Equal(t, val, string(found))
	}
}

func TestPutGetSSTable(t *testing.T) {
	lt, err := NewLSMTree(TEST_DIR, WithMemTableSize(32*1024))
	assert.Nil(t, err)
	defer cleanUp()

	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key_%d", i)
		val := []byte(fmt.Sprintf("val_%d", i))
		lt.Put(key, val)
	}
	lt.FlushMemory()

	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key_%d", i)
		val := fmt.Sprintf("val_%d", i)
		found, err := lt.Get(key)
		assert.Nil(t, err)
		assert.Equal(t, val, string(found))
	}
}

func TestPutGetDel(t *testing.T) {
	lt, err := NewLSMTree(TEST_DIR, WithMemTableSize(32*1024))
	assert.Nil(t, err)
	defer cleanUp()

	const SKIP_RATIO = 5

	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key_%d", i)
		val := []byte(fmt.Sprintf("val_%d", i))
		lt.Put(key, val)
	}

	for i := 0; i < 10000; i += SKIP_RATIO {
		key := fmt.Sprintf("key_%d", i)
		lt.Delete(key)
	}

	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key_%d", i)
		val := fmt.Sprintf("val_%d", i)
		found, err := lt.Get(key)
		assert.Nil(t, err)

		if i%SKIP_RATIO == 0 {
			assert.Equal(t, []byte(nil), found)
		} else {
			assert.Equal(t, val, string(found))
		}
	}
}

func TestAddWhileCompactingSST(t *testing.T) {
	lt, err := NewLSMTree(TEST_DIR)
	assert.Nil(t, err)
	defer cleanUp()

	// Ad-hoc, mock of "compacting".
	lt.stm.mu.RLock()
	defer lt.stm.mu.RUnlock()
	lt.stm.writeable = false

	err = lt.stm.Add(nil)
	assert.ErrorIs(t, err, InProgressError{})
}

func TestPutGetWhileCompactingSST(t *testing.T) {
	lt, err := NewLSMTree(TEST_DIR)
	assert.Nil(t, err)
	defer cleanUp()

	// Ad-hoc, mock of "compacting".
	lt.stm.mu.RLock()
	defer lt.stm.mu.RUnlock()
	lt.stm.writeable = false

	for i := 0; i < 500000; i++ {
		key := fmt.Sprintf("key_%d", i)
		val := []byte(fmt.Sprintf("val_%d", i))
		lt.Put(key, val)
	}

	for i := 0; i < 500000; i++ {
		key := fmt.Sprintf("key_%d", i)
		val := fmt.Sprintf("val_%d", i)
		found, err := lt.Get(key)
		assert.Nil(t, err)
		assert.Equal(t, val, string(found))
	}
	assert.Empty(t, lt.stm.ssTables[0])
}

func TestSaveAndLoad(t *testing.T) {
	lt, err := NewLSMTree(TEST_DIR)
	assert.Nil(t, err)
	defer cleanUp()

	for i := 0; i < 500000; i++ {
		key := fmt.Sprintf("key_%d", i)
		val := []byte(fmt.Sprintf("val_%d", i))
		lt.Put(key, val)
	}
	assert.Nil(t, lt.Close())

	lt, err = NewLSMTree(TEST_DIR)
	assert.Nil(t, err)
	assert.NotEmpty(t, lt.stm.ssTables)

	for i := 0; i < 500000; i++ {
		key := fmt.Sprintf("key_%d", i)
		val := fmt.Sprintf("val_%d", i)
		found, err := lt.Get(key)
		assert.Nil(t, err)
		assert.Equal(t, val, string(found))
	}
}
