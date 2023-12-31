package shard

import (
	"bytes"
	"crumbs/cache/lru"
	"encoding/gob"
	"hash/fnv"
	"time"
)

type ShardedCache struct {
	shards []*lru.Cache
}

func NewShardedCache(shards, entriesPerShard int, ttl time.Duration) *ShardedCache {
	caches := make([]*lru.Cache, shards)
	for i := 0; i < shards; i++ {
		caches[i] = lru.NewCache(entriesPerShard, ttl, nil)
	}

	return &ShardedCache{
		shards: caches,
	}
}

func (sc *ShardedCache) Add(key, value any) {
	shard := hash(key) % uint64(len(sc.shards))
	sc.shards[shard].Add(key, value)
}

func (sc *ShardedCache) Get(key any) (any, bool) {
	shard := hash(key) % uint64(len(sc.shards))
	return sc.shards[shard].Get(key)
}

func (sc *ShardedCache) Stats() lru.Stats {
	stats := lru.Stats{}
	for _, shard := range sc.shards {
		shardStats := shard.Stats()
		stats.Hits += shardStats.Hits
		stats.Misses += shardStats.Misses
		stats.Evicted += shardStats.Evicted
	}

	return stats
}

func (sc *ShardedCache) ResetStats() {
	for _, shard := range sc.shards {
		shard.ResetStats()
	}
}

func hash(key any) uint64 {
	var b bytes.Buffer
	gob.NewEncoder(&b).Encode(key)
	hash := fnv.New64a()
	hash.Write(b.Bytes())
	val := hash.Sum64()
	return val
}
