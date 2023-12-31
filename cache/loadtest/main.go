package main

import (
	"crumbs/cache/lru"
	"math/rand"
	"time"

	"github.com/fatih/color"
	"github.com/jamiealquiza/tachymeter"
	"github.com/pingcap/go-ycsb/pkg/generator"
	"github.com/rodaine/table"
)

type cache interface {
	Add(key, value any)
	Get(key any) (any, bool)
}

func main() {
	cacheSize := int(1e4)
	loadFactor := 100
	keys := cacheSize * int(loadFactor)
	ttl := 1 * time.Minute
	concurrency := 16

	c := lru.NewCache(cacheSize, ttl)
	// c := shard.NewShardedCache(4, 100, ttl)

	wt := tachymeter.New(&tachymeter.Config{Size: cacheSize * 5})
	rt := tachymeter.New(&tachymeter.Config{Size: cacheSize * 5})

	for i := 0; i < concurrency; i++ {
		go readWriter(c, wt, rt, keys)
	}

	ticker := time.NewTicker(5 * time.Second)
	for range ticker.C {
		headerFmt := color.New(color.FgGreen, color.Underline).SprintfFunc()
		columnFmt := color.New(color.FgYellow).SprintfFunc()
		tbl := table.
			New("Name", "Hit Rate", "Hits", "Misses", "Write P99", "Read P99").
			WithHeaderFormatter(headerFmt).
			WithFirstColumnFormatter(columnFmt)

		stats := c.Stats()
		tbl.AddRow(
			"LRU",
			float64(stats.Hits)/float64(stats.Hits+stats.Misses),
			stats.Hits,
			stats.Misses,
			wt.Calc().Time.P99,
			rt.Calc().Time.P99,
		)
		tbl.Print()

		c.ResetStats()
		wt.Reset()
		rt.Reset()
	}
}

func readWriter(c cache, wt, rt *tachymeter.Tachymeter, max int) {
	r := rand.New(rand.NewSource(time.Now().Unix()))
	g := generator.NewScrambledZipfian(0, int64(max), generator.ZipfianConstant)

	for {
		k := g.Next(r)
		start := time.Now()

		_, found := c.Get(k)
		rt.AddTime(time.Since(start))
		if !found {
			start := time.Now()
			c.Add(k, k)
			wt.AddTime(time.Since(start))
		}

		time.Sleep(1 * time.Millisecond)
	}
}
