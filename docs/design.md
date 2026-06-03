# Raft Kotlin Design

This document describes the v1 design for implementing the Raft consensus
algorithm from the extended Raft paper:
<https://raft.github.io/raft.pdf>.

The first version focuses on Core Raft: leader election, log replication, safety, 
client command submission, and a stable-storage boundary. Cluster membership changes,
snapshots, and optimized read-only requests are planned as later extensions.

## Goals

- Provide a small, embeddable Raft implementation for replicated state machines.
- Keep the Raft algorithm independent from any specific network or disk format.
- Define all inter-node RPC services and wire messages in Protocol Buffers.
- Use `kotlinx.rpc` as the Kotlin RPC library that exposes the generated
  protocol contract to Raft nodes.
- Make timing and network behavior deterministic in tests.
- Preserve Raft's safety requirements under crash-recovery assumptions by
  persisting `currentTerm`, `votedFor`, and log entries before responding to RPCs
  that depend on those updates.

## Non-Goals for v1

- No dynamic cluster membership or joint consensus.
- No log compaction or snapshot installation.
- No file-backed WAL implementation.
- No public server process, HTTP API, or gRPC transport. v1 uses `kotlinx.rpc`
  adapters over the Proto contract.
- No lease-based reads. Linearizable reads can be modeled as log commands in v1.

## Package Layout

Suggested package boundaries:

- `co.hondaya.raft`: public node API and value types.
- `co.hondaya.raft.protocol`: generated Proto messages and Kotlin mapping
  helpers.
- `co.hondaya.raft.rpc`: `kotlinx.rpc` service definitions and adapters.
- `co.hondaya.raft.storage`: stable storage interfaces and in-memory storage.
- `co.hondaya.raft.transport`: peer routing abstraction and in-memory test
  transport.

## Gradle Dependencies

Add coroutine, RPC, and protobuf dependencies when implementation starts:

- `org.jetbrains.kotlinx:kotlinx-coroutines-core`
- `org.jetbrains.kotlinx:kotlinx-coroutines-test`
- `kotlinx.rpc` runtime modules for the selected transport.
- Kotlin-compatible Protocol Buffers compiler/runtime and Gradle code generation
  plugin.

Keep concrete network and persistence dependencies out of the algorithm core.
The core may depend on generated Kotlin protocol types and `kotlinx.rpc`
interfaces, but concrete client/server bindings should live in adapters.

## Public API

### RaftNode

`RaftNode<C, R>` is the main lifecycle and client-facing API.

```kotlin
interface RaftNode<C : Any, R : Any> {
    val id: NodeId

    suspend fun start()
    suspend fun stop()

    suspend fun submit(command: C): SubmitResult<R>

    fun status(): RaftStatus
}
```

`submit` only succeeds on the current leader. Followers and candidates return a
redirect result with the most recent known leader, if available.

```kotlin
sealed interface SubmitResult<out R> {
    data class Applied<R>(val index: LogIndex, val term: Term, val result: R) :
        SubmitResult<R>

    data class NotLeader(val leaderId: NodeId?) : SubmitResult<Nothing>

    data class Unavailable(val reason: String) : SubmitResult<Nothing>
}
```

### Command Codec

The public node API is typed as `C`, but Raft RPC messages carry commands as
Proto `bytes`. Applications provide the codec at node construction time.

```kotlin
interface CommandCodec<C : Any> {
    fun encode(command: C): ByteArray
    fun decode(bytes: ByteArray): C
}
```

The codec must be deterministic and version-compatible across all nodes in the
cluster. The Raft core never inspects command contents.

### StateMachine

The state machine is deterministic and receives committed commands in log order.

```kotlin
interface StateMachine<C : Any, R : Any> {
    suspend fun apply(command: C): R
}
```

The Raft implementation owns ordering. A state machine implementation must not
assume concurrent calls to `apply`.

### RPC Service

Raft inter-node calls are exposed through `kotlinx.rpc`. The RPC service shape
must match the `.proto` contract, but the Raft algorithm should call it through a
small peer client abstraction so tests can use an in-memory implementation.

```kotlin
interface RaftPeerClient {
    suspend fun requestVote(target: NodeId, request: RequestVoteRequestProto): RequestVoteResponseProto
    suspend fun appendEntries(target: NodeId, request: AppendEntriesRequestProto): AppendEntriesResponseProto
}
```

`kotlinx.rpc` server/client setup belongs in an adapter layer. Transport
failures are treated as missing responses. A failed peer must not block progress
if a majority is reachable. Do not expose Java futures, Java streams, or
transport-specific request objects from the Raft core.

### Stable Storage

Stable storage contains all Raft state that must survive restart.

```kotlin
interface StableStorage<C : Any> {
    suspend fun load(): PersistentState<C>
    suspend fun saveTermAndVote(currentTerm: Term, votedFor: NodeId?)
    suspend fun appendEntries(entries: List<LogEntry<C>>)
    suspend fun truncateSuffix(fromIndex: LogIndex)
    suspend fun replaceLog(entries: List<LogEntry<C>>)
}
```

v1 must provide an in-memory implementation for tests and examples. File-backed
WAL can be added later behind the same interface.

### Clock and Scheduler

Timers must be injectable to avoid nondeterministic tests.

```kotlin
interface RaftScheduler {
    suspend fun delay(duration: kotlin.time.Duration)
    fun nextElectionTimeout(): kotlin.time.Duration
}
```

Production code uses randomized election timeouts and a fixed heartbeat
interval. Tests use a deterministic scheduler.

## Value Types

Use small value classes for protocol identifiers and indexes.

```kotlin
@JvmInline value class NodeId(val value: String)
@JvmInline value class Term(val value: Long)
@JvmInline value class LogIndex(val value: Long)

data class LogEntry<C : Any>(
    val index: LogIndex,
    val term: Term,
    val command: C,
)

data class ClusterConfig(
    val selfId: NodeId,
    val peers: Set<NodeId>,
    val heartbeatInterval: kotlin.time.Duration,
)
```

Log indexes follow the paper: the first real entry has index `1`; index `0` is
the virtual entry before the log and has term `0`.

When entries cross the RPC boundary, `command` is encoded into Proto `bytes`.
When entries are stored in the typed in-memory log, `command` remains `C`.

## Node State

Each node has exactly one role:

```kotlin
enum class Role {
    Follower,
    Candidate,
    Leader,
}
```

Persistent state on every node:

- `currentTerm`: latest term seen, initialized to `0`.
- `votedFor`: candidate that received this node's vote in `currentTerm`, or
  `null`.
- `log`: ordered log entries.

Volatile state on every node:

- `commitIndex`: highest log entry known to be committed, initialized to `0`.
- `lastApplied`: highest log entry applied to the state machine, initialized to
  `0`.
- `leaderId`: most recent leader known from `AppendEntries`.

Volatile state on leaders:

- `nextIndex[peer]`: next log index to send to each follower.
- `matchIndex[peer]`: highest log index known replicated on each follower.

All mutations to node state should run through a single coroutine actor, or an
equivalent single-thread-confined event loop. RPC handlers, timer events, client
submissions, and RPC responses enqueue events into that loop. This avoids
fine-grained locking around Raft state.

## Proto and RPC Protocol

All inter-node RPC services and messages are defined in `.proto` files. Kotlin
types used by the Raft core are generated from those schemas or mapped directly
from generated Proto classes.

Suggested schema layout:

- `proto/raft/v1/raft.proto`: Raft service and RPC messages.
- `proto/raft/v1/common.proto`: shared scalar wrappers such as `NodeId`, `Term`,
  `LogIndex`, and log entry metadata if a separate file is helpful.

The wire contract uses scalar values rather than Kotlin inline classes. Mapping
helpers convert between generated Proto fields and internal Kotlin value types.
Client commands are opaque bytes at the Raft protocol layer; application code
owns command encoding and result decoding.

```proto
syntax = "proto3";

package raft.v1;

service RaftPeer {
  rpc RequestVote(RequestVoteRequest) returns (RequestVoteResponse);
  rpc AppendEntries(AppendEntriesRequest) returns (AppendEntriesResponse);
}
```

The `kotlinx.rpc` adapter exposes the same operations to Kotlin callers. If the
selected `kotlinx.rpc` transport does not consume `.proto` service definitions
directly, keep `.proto` as the wire-contract source of truth and implement a
thin adapter between generated Proto messages and the `kotlinx.rpc` service
interface.

### RequestVote

```proto
message RequestVoteRequest {
  uint64 term = 1;
  string candidate_id = 2;
  uint64 last_log_index = 3;
  uint64 last_log_term = 4;
}

message RequestVoteResponse {
  uint64 term = 1;
  bool vote_granted = 2;
}
```

Receiver behavior:

- If `request.term < currentTerm`, reject.
- If `request.term > currentTerm`, update `currentTerm`, clear `votedFor`, and
  become follower before evaluating the vote.
- Grant the vote only when `votedFor` is `null` or already equals
  `candidateId`, and the candidate's log is at least as up to date as the
  receiver's log.
- Persist term/vote before returning a granted vote.
- Reset the election timer when a vote is granted.

Log freshness comparison:

- A log with a higher last log term is more up to date.
- If last log terms are equal, the log with the higher last log index is more up
  to date.

### AppendEntries

```proto
message LogEntry {
  uint64 index = 1;
  uint64 term = 2;
  bytes command = 3;
}

message AppendEntriesRequest {
  uint64 term = 1;
  string leader_id = 2;
  uint64 prev_log_index = 3;
  uint64 prev_log_term = 4;
  repeated LogEntry entries = 5;
  uint64 leader_commit = 6;
}

message AppendEntriesResponse {
  uint64 term = 1;
  bool success = 2;
}
```

Receiver behavior:

- If `request.term < currentTerm`, reject.
- If `request.term >= currentTerm`, record `leaderId`, become follower, and
  reset the election timer.
- If `request.term > currentTerm`, persist the new term and clear `votedFor`.
- Reject if the local log does not contain `prevLogIndex` with `prevLogTerm`.
- If a received entry conflicts with a local entry at the same index, delete the
  local entry and all entries after it.
- Append new entries not already present.
- If `leaderCommit > commitIndex`, set `commitIndex` to the smaller of
  `leaderCommit` and the last local log index.
- Apply newly committed entries to the state machine in order.

The same RPC is used for heartbeats with an empty `entries` list.

## Execution Model

`start()` launches these internal loops:

- Election timer loop for followers and candidates.
- Heartbeat and replication loop for leaders.
- Apply loop, or an actor event, that applies committed entries in index order.

Election timeout must be substantially larger than heartbeat interval. A
reasonable default is:

- heartbeat interval: `50ms`
- election timeout range: `150ms..300ms`

The exact defaults should be configurable in `ClusterConfig`.

## Leader Election

When a follower's election timeout elapses without receiving valid
`AppendEntries` or granting a vote:

1. Become candidate.
2. Increment `currentTerm`.
3. Vote for self and persist `currentTerm`/`votedFor`.
4. Reset election timer.
5. Send `RequestVote` to all peers.

A candidate becomes leader after receiving votes from a majority of the cluster.
Majority includes the candidate's self-vote.

If a candidate or leader observes a response or request with a higher term, it
must persist the higher term, clear `votedFor`, and become follower.

If a candidate receives valid `AppendEntries` from a leader with term equal to or
greater than its current term, it becomes follower.

On becoming leader:

- Set `nextIndex[peer]` to `lastLogIndex + 1`.
- Set `matchIndex[peer]` to `0`.
- Send an immediate heartbeat.
- Append and replicate a no-op entry for the new term. This helps the leader
  discover committed entries from previous terms and is required before
  optimized read-only requests are added.

## Log Replication

The leader handles `submit(command)` by:

1. Appending a new log entry with the current term.
2. Persisting the entry before sending replication RPCs.
3. Sending `AppendEntries` to followers using each peer's `nextIndex`.
4. Completing the submit call after the entry is committed and applied locally.

On successful follower response:

- Update `matchIndex[peer]` to the highest replicated index in the request.
- Update `nextIndex[peer]` to `matchIndex[peer] + 1`.

On failed response with the same term:

- Decrement `nextIndex[peer]` and retry from the earlier log position.
- v1 may use the simple decrement strategy from the paper summary. Conflict-term
  optimization can be added later without changing public APIs.

Commit advancement:

- For each candidate index `N > commitIndex`, if a majority of `matchIndex`
  values are at least `N` and `log[N].term == currentTerm`, set
  `commitIndex = N`.
- After `commitIndex` advances, send the updated commit index in subsequent
  heartbeats and apply entries locally in order.

The leader must not commit entries from older terms solely by counting replicas.
Older entries become committed indirectly when a current-term entry is committed.

## Applying Entries

The node applies entries when `commitIndex > lastApplied`.

For each unapplied committed entry:

1. Increment `lastApplied`.
2. Call `stateMachine.apply(log[lastApplied].command)`.
3. Complete any local submit waiter for that log index.

If a command originated on a previous leader, applying it still updates the state
machine but may not have a local submit waiter.

If `apply` fails, the node should stop and surface `Unavailable`, because Raft
cannot safely skip a committed command. Application-level retry policy belongs
inside the state machine.

## Client Semantics

Clients should submit all commands to the leader. A non-leader response includes
the most recently known `leaderId` when available.

v1 does not implement command de-duplication. Exactly-once semantics require a
client id and monotonically increasing command id stored by the state machine.
That can be layered above this library by making those fields part of `C`.

For linearizable reads in v1, callers should submit a read command through the
log. A later version may add a read-only API that first confirms leadership with
a majority heartbeat round.

## Safety Invariants

The implementation must preserve these invariants:

- A node grants at most one vote per term.
- A leader never overwrites or deletes entries in its own log.
- Followers only delete log suffixes when reconciling with a leader's
  `AppendEntries`.
- A candidate can win only if its log is at least as up to date as each voting
  follower's log.
- `currentTerm`, `votedFor`, and log mutations are persisted before dependent
  RPC responses are sent.
- `commitIndex` and `lastApplied` are monotonically increasing.
- Entries are applied exactly once and in index order on each node.

## Testing Strategy

Use in-memory storage, in-memory transport, and a deterministic scheduler for
unit and simulation tests.

Election tests:

- A follower becomes candidate after election timeout.
- A candidate with majority votes becomes leader.
- Split votes resolve after randomized timeouts.
- Any node steps down when it observes a higher term.

RequestVote tests:

- Requests from older terms are rejected.
- A node grants only one vote per term.
- Candidates with stale logs are rejected.
- Granted votes persist before response.

AppendEntries tests:

- Heartbeats reset election timeout.
- Requests with older terms are rejected.
- `prevLogIndex` and `prevLogTerm` mismatches are rejected.
- Conflicting suffixes are truncated and replaced.
- `leaderCommit` advances follower `commitIndex`.

Replication tests:

- A leader commits after majority replication.
- A minority partition cannot commit new entries.
- Followers that lag behind catch up by retrying earlier `nextIndex` values.
- Committed entries are applied in order on all reachable nodes.

Recovery tests:

- A restarted node reloads `currentTerm`, `votedFor`, and log.
- A restarted follower does not grant a second vote in the same term.
- A restarted node can catch up from the current leader.

Simulation tests:

- A three-node cluster elects one leader and commits commands.
- A five-node cluster tolerates two stopped nodes.
- A leader crash followed by a new election preserves committed commands.
- Network drops, duplicate messages, and reordering do not violate safety.

## Extension Points

### Snapshot and Log Compaction

Add `InstallSnapshot` RPC and snapshot metadata to `StableStorage`. The public
state machine API will need snapshot export/import hooks, but Core Raft should
not depend on a snapshot format.

### Membership Changes

Add joint consensus as described in the paper. Configuration changes should be
represented as special log entries and committed through the same replication
path as normal commands.

### Optimized Linearizable Reads

Add a `read` API only after implementing the paper's precautions:

- A leader has committed at least one entry from its current term.
- The leader confirms it has not been deposed by contacting a majority before
  serving the read.

### Persistent WAL

Implement a file-backed `StableStorage` with an append-only log, checksums, and
crash recovery. This must remain behind the storage interface so the algorithm
core remains testable.
