package lsm

import "time"

type LSMOption func(*LSMTree) *LSMTree

func WithSparseness(n int) LSMOption {
	return func(l *LSMTree) *LSMTree {
		l.stm.sparseness = n
		return l
	}
}

func WithErrorPct(pct float64) LSMOption {
	return func(l *LSMTree) *LSMTree {
		l.stm.errorPct = pct
		return l
	}
}

func WithMemTableSize(size int) LSMOption {
	return func(l *LSMTree) *LSMTree {
		l.memTableSize = size
		return l
	}
}

func WithMaxMemTables(max int) LSMOption {
	return func(l *LSMTree) *LSMTree {
		l.maxMemTables = max
		return l
	}
}

func WithFlushPeriod(period time.Duration) LSMOption {
	return func(l *LSMTree) *LSMTree {
		l.flushPeriod = period
		return l
	}
}
