# lsm

A basic LSM-tree key-value store written almost completely from scratch (aside from the hash function). It supports concurrent reads and writes (with non-blocking compaction!), but it is *not exactly* performant, which means I probably messed up somewhere.

Currently it

- Uses an [AA-tree](https://user.it.uu.se/~arnea/ps/simp.pdf) as the underlying balanced tree
- Uses a sparse index (per file) to speed up searches
- Uses a bloom filter to speed up searches
- Periodically flushes memtables to disk as SSTables
- Requires a manual trigger to compact **the entire first level** into the next level.

There are some other things it's missing, like

- A WAL (write-ahead-log), to make it more durable
- Better compaction schemes, see design sections for some considerations
- Compressing data files for better storage efficiency
- More/nicer debug messages, logging, and stats

Eventually, I hope to get around to implementing all of the above. But overall, it was a nice learning experience.

## Benchmarks

This is done on my machine with 1M key-value pairs.

```
benchPutKeyVals: 
260.093896ms
1922340 ops/s

benchSeqGetKeyVals: 
6.213537798s
80469 ops/s

benchRandGetKeyVals: 
6.622176223s
75504 ops/s

benchConcRandGetKeyVals:
3.595554249s
139060 ops/s
```

## Design

### Writes

The DB contains a list of **memtables** (an ordered map implementation) that is stored in memory. When a write occurs, the key-value pair is inserted into the newest memtable. If the memtable is full (exceeds a certain threshold), a new memtable is rotated in.

Older memtables are considered immutable, and are read-only. Having multiple memtables ensures that we can flush (write them to disk) in background processes without blocking writes.

When a memtable is flushed, the data is written as a **SSTable** (sorted string table), a continuous series of records in sorted order. Alongside that, we also create a **sparse index** and **bloom filter** to speed up read operations.

> In my implementation, memtables to be flushed are passed to SSTManager which does the described operations of writing to disk and creating the sparse index and bloom filter.

<!-- TODO: Insert diagram here on record format. -->

<!-- TODO: Insert diagram here on data flow. -->

#### Sparse Index

There is a sparse index for every datafile, and it maps keys to datafile offsets. Decreasing sparseness improves performance but at the cost of space.

<!-- TODO: Insert diagram here. -->

#### Bloom Filter

A [bloom filter](https://en.wikipedia.org/wiki/Bloom_filter) is a space-efficient probabilistic data structure that allows us to check for set membership. If it reports an item is not in the set, then it is guaranteed to not be in the set. Otherwise, it *may* be in the set.

This property is useful when we need to traverse across SSTables to look a for a key-value pair.

<!-- TODO: Insert diagram here. -->

### Reads

Reads will proceed by checking first the memtables, then the SSTables in reverse chronological order. It stops only when it finds a key entry in a given table, or it has iterated across every single table.

For SSTables, it does this by getting a lower and upper bound that the record *might* exists within for a given table, and then iterating over the range. Naturally, if we had to do this for every single table on disk, it would be very slow. So instead we use a bloom filter to skip tables, and reduce the number of times we have to check.

<!-- TODO: Insert diagram here. -->

### Compaction

Currently, the compaction process uses a **very simple** compaction scheme, which when triggered, compresses every SSTable in the first level into one single large table in the second table. This table is inserted at the front of the next layer to preserve reverse chronological ordering.

See this detailed [paper](https://arxiv.org/pdf/2202.04522.pdf) on various compaction designs.

### Concurrency

One of my goals for this implementation, was to support non-blocking compaction, meaning reads and writes can still be executed while the database compacts tables in level 0 to level 1.

> It does this by minimizing the amount of time the caller holds the mutex.

<!-- TODO: Insert diagram here. -->