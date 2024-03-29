package lsm

import "io"

type ReaderLengther interface {
	io.Reader
	Len() int
}

type KeyFile struct {
	Key     string
	Value   []byte
	FileIdx int
	Reader  ReaderLengther
}

type KeyFileHeap []KeyFile

func (h KeyFileHeap) Len() int {
	return len(h)
}

func (h KeyFileHeap) Less(i, j int) bool {
	if h[i].Key == h[j].Key {
		return h[i].FileIdx < h[j].FileIdx
	}
	return h[i].Key < h[j].Key
}

func (h KeyFileHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *KeyFileHeap) Push(x any) {
	*h = append(*h, x.(KeyFile))
}

func (h *KeyFileHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
