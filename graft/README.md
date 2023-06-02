# Raft

Following the original repo, a complete usage example:

```
$ go test -v -race -run TestElectionFollowerComesBack |& tee /tmp/raftlog
... logging output
... test should PASS
$ go run ./raft-testlog-viz/main.go < /tmp/raftlog
PASS TestElectionFollowerComesBack map[0:true 1:true 2:true TEST:true] ; entries: 150
... Emitted file:///tmp/TestElectionFollowerComesBack.html

PASS
```

## Sauce
- https://raft.github.io/raft.pdf
- https://eli.thegreenplace.net/2020/implementing-raft-part-1-elections
