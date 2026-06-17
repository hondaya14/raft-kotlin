# Implementation Report - 2026-06-17

## Completed

- Added coroutine dependencies for the Raft core and tests.
- Added public Raft API/value types under `co.hondaya.raft`:
  - `RaftNode`
  - `SubmitResult`
  - `CommandCodec`
  - `StateMachine`
  - `RaftStatus`
  - `NodeId`, `Term`, `LogIndex`, `LogEntry`, `ClusterConfig`
- Added `StableStorage` and `InMemoryStableStorage`.
- Added transport boundary types and `InMemoryTransport`, including missing-peer behavior, one-way drops/partitions, per-link delays that can reorder concurrent delivery, and duplicate delivery.
- Added `RaftScheduler` and `DefaultRaftScheduler`.
- Added generated-proto mapping helpers for RequestVote, AppendEntries, responses, log entries, and no-op markers.
- Added `kotlinx.rpc` generated-service adapters:
  - `KotlinxRpcRaftEndpointAdapter` exposes a `RaftPeerEndpoint` as generated `RaftService`.
  - `KotlinxRpcRaftServiceAdapter` lets the Raft core call generated peer services by `NodeId`.
- Added `CoroutineRaftNode`, a simple mailbox-based Raft node implementation with:
  - start/stop lifecycle
  - persisted term/vote loading
  - RequestVote handling
  - AppendEntries handling
  - election timeout handling
  - leader transition after majority votes
  - commandless leader-election no-op log entry append/persist/replicate/apply-skip
  - long-lived leader-side per-peer AppendEntries workers
  - follower log reconciliation
  - commit-index advancement for current-term entries
  - apply loop for committed commands
  - applied command results routed back through the Raft loop mailbox
  - client submit results after apply
  - follower `NotLeader(leaderId)` redirects
  - injectable coroutine context for deterministic simulations/tests
- Added focused tests for:
  - granted vote persistence
  - RequestVote older-term rejection
  - stale candidate rejection
  - AppendEntries persistence and apply
  - AppendEntries older-term rejection
  - AppendEntries previous-log mismatch rejection
  - conflicting follower suffix truncation and replacement
  - duplicate AppendEntries applying a command only once
  - state-machine apply failure completing submit with `Unavailable`
  - follower submit redirect
  - single-node leader election and submit
  - virtual-time election using an injected coroutine dispatcher
  - candidate stepdown on valid same-term AppendEntries
  - three-node in-memory election, replication, commit, and apply
  - five-node cluster tolerating two stopped nodes
  - restarted follower catch-up from the current leader
  - minority partition unable to commit a new entry
  - leader crash followed by a new election preserving committed commands
  - leader stepdown and higher-term persistence after a peer response
  - restarted node reload of persistent term, vote, and log
  - restarted follower refusing a second vote in the same term
  - persistence ordering for granted votes, AppendEntries success, truncation, higher-term replies, and leader client-entry replication
- Added proto mapping round-trip tests for RequestVote and AppendEntries.
- Added generated `kotlinx.rpc` adapter tests for endpoint and peer-client directions.
- Added in-memory transport tests for delayed delivery, reordered concurrent delivery, and duplicate delivery.
- Updated `docs/design.md` to document the `LogEntry.noOp` / proto `no_op` marker used by the implementation.

## Verification

- `./gradlew test` passed.
- `./gradlew build` passed.

## Deferred / Non-Goal Items

- No concrete network bootstrap/server process is included. This matches the v1 non-goal of no public server process, HTTP API, or direct gRPC transport.
- File-backed WAL, snapshots, membership changes, optimized read-only requests, and command de-duplication are not implemented. These are documented v1 non-goals or extension points.

## Follow-up Completed After Initial Report

- Replaced one-off leader replication jobs with long-lived per-peer AppendEntries worker coroutines.
- Added a focused test for follower conflicting suffix truncation and replacement.
- Added an injectable coroutine context to `CoroutineRaftNode` so deterministic tests can run the node on a controlled dispatcher.
- Added leader-election no-op entries and a focused test that they are persisted and skipped by the state machine.
- Added `no_op = 4` to the proto log entry contract.
- Added core-to-generated-proto mapping helpers and round-trip tests.
- Added thin adapters between the generated `kotlinx.rpc` `RaftService` and the algorithm-facing transport/endpoint interfaces.
- Added recovery tests for persistent term/vote/log reload and same-term vote refusal after restart.
- Added a virtual-time election test using the injected coroutine context.
- Added simulation-style tests for restarted follower catch-up, minority partition commit refusal, and leader crash followed by a new election.
- Added a focused higher-term response test that verifies leader stepdown and stable term/vote persistence.
- Added per-link delay support to `InMemoryTransport` and tests proving delayed and reordered concurrent delivery.
- Added duplicate-delivery support to `InMemoryTransport` and tests proving duplicate delivery plus duplicate AppendEntries idempotence.
- Added explicit tests for candidate stepdown on valid same-term AppendEntries and five-node tolerance of two stopped nodes.
- Removed the application-level no-op command requirement; no-op log entries now store `command = null`, use empty wire command bytes, and are skipped by the apply loop.
- Added focused tests for older-term vote rejection, older-term AppendEntries rejection, previous-log mismatch rejection, and state-machine failure surfacing as `Unavailable`.
- Added explicit persistence-ordering tests for the five invariants listed in `docs/design.md`.
- Stabilized the higher-term leader stepdown test with a one-shot election scheduler so it verifies the stepdown before a new election can start.
- Updated `docs/design.md` for the no-op protocol marker.
- Re-ran `./gradlew build` successfully after these changes.
