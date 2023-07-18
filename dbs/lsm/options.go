package lsm

type LSMOption func(*LSMTree) *LSMTree

func WithSparseness(n int) LSMOption {
	return func(l *LSMTree) *LSMTree {
		l.sparseness = n
		return l
	}
}

func WithErrorPct(pct float64) LSMOption {
	return func(l *LSMTree) *LSMTree {
		l.errorPct = pct
		return l
	}
}
