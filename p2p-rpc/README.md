# p2p-rpc

This is a toy implementation of a peer-to-peer (p2p) network over gRPC using Consul for service discovery.

Currently it does nothing aside from printing out the responses from other peers in the network.

To run,
```
go run p2p-rpc/main.go <name> <addr> <consul-addr>
```