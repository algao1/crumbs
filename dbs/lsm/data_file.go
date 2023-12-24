package lsm

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

type keyValue struct {
	key   []byte
	value []byte
}

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

func readChunk(file io.ReaderAt, offset, size int) ([]byte, error) {
	b := make([]byte, size)
	_, err := file.ReadAt(b, int64(offset))
	if err != nil {
		return nil, fmt.Errorf("unable to read chunk: %w", err)
	}
	return b, nil
}

func readKeyValue(reader io.Reader) (keyValue, int, error) {
	kb, kSize, err := readElement(reader)
	if err != nil {
		return keyValue{}, 0, fmt.Errorf("unable to read key: %w", err)
	}

	vb, vSize, err := readElement(reader)
	if err != nil {
		return keyValue{}, 0, fmt.Errorf("unable to read value: %w", err)
	}

	return keyValue{key: kb, value: vb}, kSize + vSize, nil
}

func readElement(reader io.Reader) ([]byte, int, error) {
	lb := make([]byte, 8)
	_, err := reader.Read(lb)
	if err != nil {
		return nil, 0, fmt.Errorf("unable to read length: %w", err)
	}

	l, n := binary.Varint(lb)
	if n <= 0 {
		return nil, 0, fmt.Errorf("unable to decode length of binary")
	}
	if l < 0 {
		return nil, 0, fmt.Errorf("unexpectedly got negative length: %d", l)
	}

	b := make([]byte, l)
	_, err = reader.Read(b)
	if err != nil {
		return nil, 0, fmt.Errorf("unable to read value: %w", err)
	}

	return b, int(8 + l), nil
}
