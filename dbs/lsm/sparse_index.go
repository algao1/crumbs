package lsm

import (
	"encoding/gob"
	"fmt"
	"os"
)

type recordOffset struct {
	Key    string
	Offset int
}

type SparseIndex struct {
	Index []recordOffset
}

func NewSparseIndex() *SparseIndex {
	return &SparseIndex{
		Index: make([]recordOffset, 0),
	}
}

func (si *SparseIndex) Append(ro recordOffset) {
	si.Index = append(si.Index, ro)
}

func (si *SparseIndex) GetOffsets(key string) (int, int) {
	left, right := si.findBounds(key)
	if right >= len(si.Index) {
		return si.Index[left].Offset, -1
	}
	return si.Index[left].Offset, si.Index[right].Offset
}

func (si *SparseIndex) findBounds(key string) (int, int) {
	n := len(si.Index)

	// Find the greatest lower bound.
	left, right := 0, n-1
	for left < right {
		mid := left + (right-left)/2

		if si.Index[mid].Key < key {
			left = mid + 1
		} else {
			right = mid
		}
	}
	lowerBound := left
	if lowerBound > 0 {
		lowerBound--
	}

	// Find the lowest greater bound.
	upperBound := left + 1
	for upperBound < n && si.Index[upperBound].Key <= key {
		upperBound++
	}

	return lowerBound, upperBound
}

func (si *SparseIndex) Encode(filename string) error {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("unable to open file for sparse index: %w", err)
	}
	defer file.Close()

	err = gob.NewEncoder(file).Encode(*si)
	if err != nil {
		return fmt.Errorf("unable to encode sparse index: %w", err)
	}
	return nil
}

func (si *SparseIndex) Decode(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("unable to open file for sparse index: %w", err)
	}
	defer file.Close()

	sparseIndex := NewSparseIndex()
	err = gob.NewDecoder(file).Decode(&sparseIndex)
	if err != nil {
		return fmt.Errorf("unable to decode sparse index: %w", err)
	}
	*si = *sparseIndex

	return nil
}
