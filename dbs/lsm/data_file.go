package lsm

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
)

type keyValue struct {
	key   []byte
	value []byte
}

type bufferedWriter struct {
	*bufio.Writer
}

func (bw *bufferedWriter) writeKeyVal(key string, val []byte) (int, error) {
	lb := make([]byte, 16)
	keyb := []byte(key)

	binary.PutVarint(lb[:8], int64(len(keyb)))
	binary.PutVarint(lb[8:], int64(len(val)))

	bytesWritten := 0
	n, err := bw.Write(lb)
	if err != nil {
		return 0, fmt.Errorf("unable to write length: %w", err)
	}
	bytesWritten += n

	n, err = bw.Write(keyb)
	if err != nil {
		return 0, fmt.Errorf("unable to write key: %w", err)
	}
	bytesWritten += n

	n, err = bw.Write(val)
	if err != nil {
		return 0, fmt.Errorf("unable to write val: %w", err)
	}
	bytesWritten += n

	return bytesWritten, nil
}

func readChunk(file io.ReaderAt, offset, size int) ([]byte, error) {
	b := make([]byte, size)
	return readChunkWithBuffer(file, offset, b)
}

func readChunkWithBuffer(file io.ReaderAt, offset int, b []byte) ([]byte, error) {
	_, err := file.ReadAt(b, int64(offset))
	if err != nil {
		return nil, fmt.Errorf("unable to read chunk: %w", err)
	}
	return b, nil
}

func readKeyVal(reader io.Reader) (keyValue, int, error) {
	lb := make([]byte, 16)
	_, err := reader.Read(lb)
	if err != nil {
		return keyValue{}, 0, fmt.Errorf("unable to read length: %w", err)
	}

	l1, n1 := binary.Varint(lb[:8])
	if n1 <= 0 {
		return keyValue{}, 0, fmt.Errorf("unable to decode length of binary")
	}
	if l1 < 0 {
		return keyValue{}, 0, fmt.Errorf("unexpectedly got negative length: %d", l1)
	}

	l2, n2 := binary.Varint(lb[8:])
	if n2 <= 0 {
		return keyValue{}, 0, fmt.Errorf("unable to decode length of binary")
	}
	if l2 < 0 {
		return keyValue{}, 0, fmt.Errorf("unexpectedly got negative length: %d", l2)
	}

	b := make([]byte, l1+l2)
	_, err = reader.Read(b)
	if err != nil {
		return keyValue{}, 0, fmt.Errorf("unable to read value: %w", err)
	}

	return keyValue{
		key:   b[:l1],
		value: b[l1:],
	}, int(l1 + l2), nil
}
