# 🍞 crumbs

**(Bread)crumbs of knowledge**. A collection of small-ish, playground projects; mostly for learning purposes and for fun. Probably will make a blog out of these when I get the time.

### Cache

-   **lru** - A simple, concurrent LRU cache.
-   **sharded lru** - A sharded LRU cache.

### Database

-   **keg** - A simple no-dependency disk-based key-value store, based on [Bitcask](https://github.com/basho/bitcask).
-   **lsm** - A basic LSM-tree based key-value store, built mostly from scratch and performs ok. Inspired in part by this well-written [post](https://artem.krylysov.com/blog/2023/04/19/how-rocksdb-works/).

### Go

-   **1brc** - My implementation of the [One Billion Row Challenge](https://www.morling.dev/blog/one-billion-row-challenge/), mostly following the optimizations [here](https://benhoyt.com/writings/go-1brc/).
-   **coro** - Implementing coroutines following Russ Cox's [blog](https://research.swtch.com/coro).
-   **dst** - A unsuccessful attempt at creating a deterministic simulation tester for Go (see [Reverie](https://github.com/facebookexperimental/reverie)).
-   **load-shed** - Implementation of Uber's [Cinnamon Load Shedder](https://www.uber.com/blog/cinnamon-using-century-old-tech-to-build-a-mean-load-shedder/?uclick_id=0f04f49d-7cb2-4574-b24f-0fafec19a257), with some liberties taken.

### Misc

-   **p2p-rpc** - A toy implementation of a peer-to-peer (p2p) network over gRPC using [Consul](https://github.com/hashicorp/consul) for service discovery.
