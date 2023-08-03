package lsm

import (
	"encoding/gob"
	"fmt"
	"os"
)

type Meta struct {
	Level int
}

func (m *Meta) Encode(filename string) error {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("unable to open file for metadata: %w", err)
	}
	defer file.Close()

	err = gob.NewEncoder(file).Encode(m)
	if err != nil {
		return fmt.Errorf("unable to encode metadata: %w", err)
	}
	return nil
}

func (m *Meta) Decode(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("unable to open metadata file: %w", err)
	}
	defer file.Close()

	var nm Meta
	err = gob.NewDecoder(file).Decode(&nm)
	if err != nil {
		return fmt.Errorf("unable to decode bloom filter: %w", err)
	}
	return nil
}
