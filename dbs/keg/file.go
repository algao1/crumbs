package keg

import "io"

const MAX_FILE_SIZE = 1024 * 1024 * 1024 * 2 // 2GB

type Hint struct {
	FileID      uint32
	ValueOffset uint32
	ValueSize   uint32
}

type ActiveFile struct {
	Writer io.WriteCloser
	Reader io.ReaderAt
	FileID uint32
	Offset uint32
}

type StaleFile struct {
	Reader io.ReaderAt
	FileID uint32
}
