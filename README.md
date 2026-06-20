# raft-kotlin
Implementation of the Raft consensus algorithm in Kotlin

## Docker Compose demo

Build and run three independent Raft node processes that communicate over gRPC:

```sh
docker compose up --build
```

Each container starts one node. The log prints the node id, state, term, leader id, and log positions once per second. A healthy run should converge to one `LEADER` and two `FOLLOWER` nodes after election.
