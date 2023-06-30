package keg

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"unsafe"
)

const HEADER_SIZE = uint32(unsafe.Sizeof(Header{}))

type Record struct {
	Header Header
	Value  []byte
	Key    []byte
}

type Header struct {
	Timestamp uint32
	Expiry    uint32
	KeySize   uint32
	ValueSize uint32
}

func (h *Header) encode(buf *bytes.Buffer) error {
	return binary.Write(buf, binary.LittleEndian, h)
}

func (h *Header) decode(buf *bytes.Buffer) error {
	return binary.Read(buf, binary.LittleEndian, h)
}

// readRecord reads and returns a record from the given reader at the given offset.
func readRecord(reader io.ReaderAt, offset uint32) (Record, error) {
	hb := make([]byte, HEADER_SIZE)
	n, err := reader.ReadAt(hb, int64(offset))
	if err != nil {
		return Record{}, fmt.Errorf("unable to read header from file at %d: %w", offset, err)
	}
	if n != int(HEADER_SIZE) {
		return Record{}, fmt.Errorf("incorrect number of bytes read: %d", n)
	}

	h := Header{}
	if err := h.decode(bytes.NewBuffer(hb)); err != nil {
		return Record{}, fmt.Errorf("unable to decode header: %w", err)
	}

	start := offset + HEADER_SIZE
	value := make([]byte, h.ValueSize)

	if h.ValueSize > 0 {
		n, err = reader.ReadAt(value, int64(start))
		if err != nil {
			return Record{}, fmt.Errorf("unable to read value from file: %w", err)
		}
		if n != int(h.ValueSize) {
			return Record{}, fmt.Errorf("incorrect number of bytes read: %d", n)
		}
	}

	start += h.ValueSize
	key := make([]byte, h.KeySize)

	n, err = reader.ReadAt(key, int64(start))
	if err != nil {
		return Record{}, fmt.Errorf("unable to read key from file: %w", err)
	}
	if n != int(h.KeySize) {
		return Record{}, fmt.Errorf("incorrect number of bytes read: %d", n)
	}

	return Record{
		Header: h,
		Value:  value,
		Key:    key,
	}, nil
}
