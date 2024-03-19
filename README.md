# 🍞 crumbs

**(Bread)crumbs of knowledge**. A collection of smallish, playground projects; mostly for learning purposes and for fun. Probably will make a blog out of these when I get the time.

### Cache

-   **LRU** - A simple, concurrent LRU cache
-   **Sharded LRU** - A sharded LRU cache

### Database

-   **Keg** - A simple no-dependency disk-based key-value store, based on [Bitcask](https://github.com/basho/bitcask).
-   **LSM** - A basic LSM-tree based key-value store, built mostly from scratch and performs ok. Inspired in part by this well-written [post](https://artem.krylysov.com/blog/2023/04/19/how-rocksdb-works/).

### Go

-   **1brc** - My implementation of the [One Billion Row Challenge](https://www.morling.dev/blog/one-billion-row-challenge/), mostly following the optimizations [here](https://benhoyt.com/writings/go-1brc/)
-   **coro** - Implementing coroutines following Russ Cox's [blog](https://research.swtch.com/coro)
-   **dst** - My (currently) failed attempt at creating a deterministic simulation tester for Go, realized that there's no good way to make everything deterministic without something like [Reverie](https://github.com/facebookexperimental/reverie)

### Misc

-   **p2p_rpc** - A toy implementation of a peer-to-peer (p2p) network over gRPC using [Consul](https://github.com/hashicorp/consul) for service discovery.
