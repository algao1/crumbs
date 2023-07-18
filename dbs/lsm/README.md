# lsm

A basic LSM-tree based database written mostly from scratch (aside from the hash function). It supports concurrent operations, but it is *not exactly* performant, which means I probably messed up somewhere.

Currently it

- Uses an [AA-tree](https://user.it.uu.se/~arnea/ps/simp.pdf) as the underlying balanced tree
- Uses a sparse index (per file) to speed up searches
- Uses a bloom filter to speed up searches
- Periodically flushes memtables to disk as SSTables

There are some other things it's currently missing, like

- Compaction (level, size, or some combination of both)
- Cache for SSTables to improve sequential reads
- A WAL (write-ahead-log), to make it more durable
- Nicer debug messages/logging, and stats

Eventually, I hope to get around to implementing all of the above. But overall, it was a nice learning experience.

## Benchmarks

This is done on my machine with 1M key/val pairs.

```
benchPutKeyVals: 
	184.424624ms
    1807097 ops/s

benchSeqGetKeyVals: 
	4.158360967s
    80159 ops/s

benchRandGetKeyVals: 
	4.500721048s
    74058 ops/s
```

## Design

TODO.

See [paper](https://arxiv.org/pdf/2202.04522.pdf) on various compaction designs.