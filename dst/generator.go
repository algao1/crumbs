package dst

import "math/rand"

type Generator struct {
	r rand.Source
}

func NewGenerator(seed int64) *Generator {
	return &Generator{
		r: rand.NewSource(seed),
	}
}

func (g *Generator) Rand() int {
	v := int(g.r.Int63())
	return v
}
