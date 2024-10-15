# KegDB

KegDB is a simple no-dependency disk-based key-value store (under 1K LOC) based on [Bitcask](https://riak.com/assets/bitcask-intro.pdf) that is, _gasp_, not **blazingly fast**. It is meant purely as an educational project to learn more about how databases work.

Currently, it supports the following operations:

-   Put
-   Get
-   Delete
-   Fold
-   Compact
-   Close

## Design

KegDB maintains a collection of stale files and an active file. The files store the actual keys and values, and an in-memory key directory maps keys to file offsets. Writes to KegDB appends to the end of the active file and also updates the key directory with the file offset. The active file is rotated out once it is full, and transitions to a stale state (read-only).

Each record has the following binary format

```
+-----------+--------+----------+------------+-------+-----+
| Timestamp | Expiry | Key Size | Value Size | Value | Key |
+-----------+--------+----------+------------+-------+-----+
```

The first three entries are considered part of the header. The checksum (CRC) is left out of the implementation for simplicity's sake, but having it would allow us to check the integrity of the data.

### Compaction

Because deletions just appends a tombstone to the end of the file, it fragments the data. The compaction process currently

1. Iterates over all the stale keys in the key directory
2. Writes them to a new, temporary KegDB instance
3. Moves the `.keg` files over and updates the key directory with the updated location and offsets
4. For each compacted file, a `.hint` file is generated which is just a key directory for kv-pairs in that file

### Hint Files

Hint files help speed up the initialization times for KegDB by skipping the process of decoding the records, and instead only loading the key directory for that file.

## Plans

There are a few things I want to add at some point

-   Expiry using the `Timestamp` field
-   Add `CRC` for integrity checks
-   Add options to customize maximum file sizes, ignore hint files, and automatic/periodic compactions

I also hope to use this project in some future work, maybe using a consensus algorithm (i.e. Raft) to build a distributed kv-store.
