package lsm

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

func flushBytes(file *os.File, b []byte) (int, error) {
	lb := make([]byte, 8)
	binary.PutVarint(lb, int64(len(b)))

	total := 0
	n, err := file.Write(lb)
	if err != nil {
		return 0, fmt.Errorf("unable to write length: %w", err)
	}
	total += n

	n, err = file.Write(b)
	if err != nil {
		return 0, fmt.Errorf("unable to write contents: %w", err)
	}
	total += n

	return total, nil
}

func readBytes(file io.ReaderAt, offset int64) ([]byte, error) {
	lb := make([]byte, 8)
	_, err := file.ReadAt(lb, offset)
	if err != nil {
		return nil, fmt.Errorf("unable to read length: %w", err)
	}

	l, n := binary.Varint(lb)
	if n <= 0 {
		return nil, fmt.Errorf("unable to decode length of binary")
	}

	b := make([]byte, l)
	_, err = file.ReadAt(b, offset+8)
	if err != nil {
		return nil, fmt.Errorf("unable to read value: %w", err)
	}

	return b, nil
}
