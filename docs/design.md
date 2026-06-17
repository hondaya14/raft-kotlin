# Raft Kotlin Design

This document describes the v1 design for implementing the Raft consensus
algorithm from the extended Raft paper:
<https://raft.github.io/raft.pdf>.

The first version focuses on Core Raft: leader election, log replication, safety,
client command submission, and a stable-storage boundary. Cluster membership changes,
snapshots, and optimized read-only requests are planned as later extensions.

## Goals

- Provide a small, embeddable Raft implementation for replicated state machines.
- Structure the implementation so it maps directly onto the extended Raft paper's
  Figure 2 "Rules for Servers" and Sections 5–8. Do not introduce intermediate
  abstractions that are not in the paper (for example, etcd's `Ready`/`Advance`
  batches); each event handler should correspond to a rule the paper defines.
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
- No runtime-switchable RPC wire format. v1 fixes the wire to Protocol Buffers
  carried by `kotlinx.rpc`. Alternative transports can be added later behind the
  `RaftService` abstraction without changing the wire.

## Package Layout

Suggested package boundaries:

- `co.hondaya.raft`: public node API (`RaftNode`), value types, and the
  application-facing contracts (`StateMachine`, `CommandCodec`).
- `co.hondaya.raft.loop`: the Raft loop, its mailbox events, and the handlers
  that implement Figure 2 rules. All Raft state (`currentTerm`, `votedFor`,
  `log`, `commitIndex`, `lastApplied`, `nextIndex`, `matchIndex`) lives here.
- `co.hondaya.raft.storage`: `StableStorage` interface and `InMemoryStableStorage`.
- `co.hondaya.raft.transport`: `RaftService` abstraction, `Replicator`
  coroutine, and `InMemoryTransport` for tests.
- `co.hondaya.raft.protocol`: generated Proto messages and core ↔ Proto
  mapping helpers.
- `co.hondaya.raft.rpc`: `kotlinx.rpc` `RaftService` definitions and the
  adapters that forward inbound calls into the Raft loop mailbox.

## Abstraction Boundaries

Three layers sit between an application command and the wire. Each layer has a
single responsibility; v1 makes deliberate choices about which layers are
pluggable and which are fixed.

```
┌──────────────────────────────────────────────────────────────┐
│ Application: domain command of type C                        │
└──────────────────────────────────────────────────────────────┘
                  │  CommandCodec<C>            ← (1) user-supplied
                  ▼
┌──────────────────────────────────────────────────────────────┐
│ Raft loop: log entries store the command as opaque ByteArray │
└──────────────────────────────────────────────────────────────┘
                  │  RaftService             ← (2) transport abstraction
                  ▼
┌──────────────────────────────────────────────────────────────┐
│ kotlinx.rpc adapter: Proto wire (fixed in v1)                │
└──────────────────────────────────────────────────────────────┘
                  │
                  ▼
              gRPC / in-memory / future transports
```

1. **`CommandCodec<C>`** — application domain type ↔ `ByteArray`. The Raft loop
   never inspects command contents; it only carries the encoded bytes through
   the log and into `AppendEntries`. Applications choose how to encode (Proto,
   JSON, `kotlinx.serialization`, …). The codec must be deterministic and
   identical across all nodes in the cluster.
2. **`RaftService`** — Kotlin interface for "call `RequestVote` /
   `AppendEntries` on a remote peer". Implementations:
    - `InMemoryRaftService` for tests and deterministic simulations.
    - `KotlinxRpcRaftService` for production over `kotlinx.rpc`.
    - Other transports (gRPC direct, HTTP, …) can be added without touching the
      Raft loop or the Proto schema.
3. **Wire format** — fixed in v1 to Protocol Buffers carried by `kotlinx.rpc`.
   This is the only knob v1 intentionally does not expose: there is one
   reference wire to validate safety against before opening it up. A future
   alternative wire would be added as a parallel implementation behind
   `RaftService`, not as a runtime switch inside the existing one.

The Raft loop depends only on (1) and (2). The wire format is an implementation
detail of one particular `RaftService`.

## Main Use-Case Diagrams

The diagrams below show the primary v1 flows at the boundary between
application code, the Raft loop, stable storage, the state machine, and peer
nodes. They are intentionally implementation-oriented: each message maps to a
public API call, mailbox event, storage operation, or Raft RPC described later
in this document.

### Client command submission

```mermaid
sequenceDiagram
    autonumber
    participant Client
    participant Node as RaftNode
    participant Loop as Raft loop
    participant Storage as StableStorage
    participant Peers as Peer nodes
    participant Apply as Apply loop
    participant SM as StateMachine

    Client->>Node: submit(command)
    Node->>Loop: ClientSubmit(command)
    alt node is not leader
        Loop-->>Node: NotLeader(leaderId)
        Node-->>Client: SubmitResult.NotLeader
    else node is leader
        Loop->>Storage: appendEntries([entry])
        Storage-->>Loop: persisted
        Loop->>Peers: AppendEntries(entry)
        Peers-->>Loop: AppendEntriesResponse(success)
        Loop->>Loop: advance commitIndex after majority
        Loop->>Apply: committed range
        Apply->>SM: apply(decoded command)
        SM-->>Apply: result
        Apply-->>Loop: Applied(through = index)
        Apply-->>Node: complete pending submit
        Node-->>Client: SubmitResult.Applied
    end
```

### Leader election

```mermaid
sequenceDiagram
    autonumber
    participant Timer as Election timer
    participant Loop as Raft loop
    participant Storage as StableStorage
    participant Peers as Peer nodes
    participant Repl as Replicators

    Timer->>Loop: ElectionTimeout
    Loop->>Loop: become CANDIDATE, increment currentTerm, vote for self
    Loop->>Storage: saveTermAndVote(currentTerm, selfId)
    Storage-->>Loop: persisted
    Loop->>Peers: RequestVote(term, lastLogIndex, lastLogTerm)
    Peers-->>Loop: RequestVoteResponse(voteGranted)
    alt majority granted
        Loop->>Loop: become LEADER
        Loop->>Repl: start one replicator per peer
        Repl->>Peers: AppendEntries(empty heartbeat)
    else higher term observed
        Loop->>Storage: saveTermAndVote(higherTerm, null)
        Storage-->>Loop: persisted
        Loop->>Loop: become FOLLOWER
    end
```

### Log replication and follower catch-up

```mermaid
sequenceDiagram
    autonumber
    participant Repl as Leader replicator
    participant Follower
    participant Loop as Leader raft loop

    Repl->>Follower: AppendEntries(prevLogIndex, prevLogTerm, entries)
    alt follower log matches prev entry
        Follower-->>Repl: success = true
        Repl-->>Loop: AppendEntriesResponseReceived(success)
        Loop->>Loop: update matchIndex and nextIndex
        Loop->>Loop: advance commitIndex if majority replicated
    else follower log does not match
        Follower-->>Repl: success = false
        Repl-->>Loop: AppendEntriesResponseReceived(failure)
        Loop->>Loop: decrement nextIndex for follower
        Loop-->>Repl: retry from earlier index
    end
```

### Restart and recovery

```mermaid
sequenceDiagram
    autonumber
    participant Runtime
    participant Node as RaftNode
    participant Storage as StableStorage
    participant Loop as Raft loop
    participant Timer as Timers

    Runtime->>Node: start()
    Node->>Storage: load()
    Storage-->>Node: PersistentState(currentTerm, votedFor, log)
    Node->>Loop: initialize persistent and volatile state
    Node->>Timer: start election and heartbeat timers
    alt no valid leader contacts this node
        Timer->>Loop: ElectionTimeout
        Loop->>Loop: start election with recovered term/log
    else leader sends AppendEntries
        Loop->>Loop: record leaderId and reconcile log
    end
```

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

Per the Raft paper Section 8 ("Client interaction"), `submit` does not return a
result until the command has been committed *and* applied to the state machine.
The function is therefore `suspend`: it stays suspended until that point. Callers
that need to pipeline multiple commands can wrap each call in `async { … }`; each
submission independently waits for its own commit-and-apply.

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
interface RaftService {
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
@JvmInline
value class NodeId(val value: String)
@JvmInline
value class Term(val value: Long)
@JvmInline
value class LogIndex(val value: Long)

data class LogEntry<C : Any>(
    val index: LogIndex,
    val term: Term,
    val command: C?,
    val noOp: Boolean = false,
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
enum class NodeState {
    FOLLOWER,
    CANDIDATE,
    LEADER,
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

All of the state above is owned by a single coroutine — referred to in this
document as the **Raft loop** — that lives in `co.hondaya.raft.loop`. RPC
handlers, timer events, client submissions, RPC responses, and apply-loop
completions enqueue events into the Raft loop's mailbox; the loop is the only
code path that reads or writes these fields. This is the JVM-side counterpart of
the implicit "one server, one decision at a time" assumption of Figure 2: it
guarantees that a `currentTerm` bump and its `votedFor` reset are observed
atomically by every rule that reads them.

The Raft loop is described in detail in "Server Implementation Structure" below.

## Server Implementation Structure

The implementation is built around three long-lived coroutines per node. Only
the Raft loop owns mutable Raft state; the other two are IO workers that take
strict input from, and send strict feedback to, the Raft loop.

```
                            ┌─────────────────────────┐
   client submit  ──────────▶                         │
   RPC inbound    ──────────▶      Raft loop          │  ← owns all Raft state
   RPC reply      ──────────▶  (single coroutine)     │
   timer events   ──────────▶                         │
                            └────────┬────────────────┘
                                     │
                ┌────────────────────┼─────────────────────┐
                ▼                    ▼                     ▼
        ┌──────────────┐    ┌─────────────────┐    ┌──────────────┐
        │ StableStorage│    │  Replicator(s)  │    │ apply loop   │
        │ (suspending) │    │ (one per peer,  │    │ (sequential, │
        │              │    │  leader only)   │    │  per node)   │
        └──────────────┘    └─────────────────┘    └──────────────┘
```

### Raft loop

A single coroutine consuming a `Channel<RaftEvent>` mailbox. Each iteration:

1. Apply the Figure 2 "All Servers" precondition:
   *"If RPC request or response contains term T > currentTerm: set
   currentTerm = T, convert to follower."*
2. Dispatch to the handler that matches the event (`handleAppendEntries`,
   `handleRequestVote`, `startElection`, `sendHeartbeatsIfLeader`, etc.).
3. Apply the Figure 2 "All Servers" postcondition:
   *"If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied]
   to state machine."* In this implementation that step hands the committed
   range to the apply loop and waits for an `Applied(through)` event before
   bumping `lastApplied`.

Handlers may call suspending `StableStorage` functions. While the loop is
suspended on disk IO, peer responses and timer ticks continue to arrive in the
mailbox in arrival order; the loop processes them as soon as the current handler
returns.

### Replicator coroutines (leader only)

When the loop transitions to leader, it launches one `Replicator(peerId)`
coroutine per peer. Each Replicator:

- Reads the entries it needs to send from a shared snapshot of the log together
  with the leader's view of `nextIndex[peer]`.
- Sends `AppendEntries` RPCs via `RaftService`. Heartbeats are
  zero-entry `AppendEntries` calls (Figure 2 "Leaders" rule: "Upon election:
  send initial empty AppendEntries RPCs (heartbeat) to each server; repeat
  during idle periods to prevent election timeouts").
- Posts the resulting `AppendEntriesResponse` back into the Raft loop mailbox
  as an `AppendEntriesResponseReceived` event. The loop is the only place that
  updates `nextIndex` / `matchIndex` / `commitIndex`.
- Receives a cancellation signal when the node steps down. All Replicators are
  cancelled before the loop transitions out of leader state.

This is the implementation-level expression of *"If last log index ≥ nextIndex
for a follower: send AppendEntries RPC with log entries starting at nextIndex"*
(Figure 2, Leaders). Per-peer coroutines let one slow follower never block
replication to the others.

### Apply loop

A single coroutine, started in `start()` and stopped in `stop()`, drains a
`Channel<CommittedRange>`. For each range, it iterates `lastApplied + 1 ..
commitIndex` (the indexes are recomputed from the snapshot it was handed; it does
not read the loop's mutable state), calls `StateMachine.apply(decode(entry))` in
order, and:

1. Completes the `CompletableDeferred` for any local `submit` waiter at that
   index.
2. Sends an `Applied(through: LogIndex)` event back to the Raft loop so the loop
   can advance `lastApplied`.

Keeping apply off the Raft loop ensures that a slow state machine cannot stall
heartbeats, vote handling, or further commit advancement. The serialization
guarantee Raft requires (commands applied in index order, exactly once per node)
is preserved by the single apply-loop coroutine.

### Election and heartbeat timers

Two tiny coroutines convert wall-clock time into events:

- An election timer that, when no valid `AppendEntries` or granted vote has
  arrived within `RaftScheduler.nextElectionTimeout()`, emits `ElectionTimeout`
  into the mailbox. The loop resets this timer whenever it processes such an
  event.
- A heartbeat ticker that, while this node is leader, emits `HeartbeatTick` once
  every `heartbeatInterval`. The loop turns each tick into a fresh round of
  heartbeats by waking the Replicators.

Both timers use `RaftScheduler.delay(…)` so tests can drive them with virtual
time.

## Raft Loop Events

The mailbox alphabet is the union of arrows in Figure 2 plus client submission
and apply-loop feedback. There is intentionally no aggregating event ("commit
batch", "ready", etc.); every event corresponds 1:1 to something the paper
already describes.

```kotlin
sealed interface RaftEvent {
    // AppendEntries RPC (Figure 2: "AppendEntries RPC > Receiver implementation")
    data class AppendEntriesReceived(
        val req: AppendEntriesRequest,
        val reply: CompletableDeferred<AppendEntriesResponse>,
    ) : RaftEvent

    // RequestVote RPC (Figure 2: "RequestVote RPC > Receiver implementation")
    data class RequestVoteReceived(
        val req: RequestVoteRequest,
        val reply: CompletableDeferred<RequestVoteResponse>,
    ) : RaftEvent

    // Responses to RPCs this node sent out (drive the Leaders / Candidates rules)
    data class AppendEntriesResponseReceived(
        val from: NodeId,
        val req: AppendEntriesRequest,
        val res: AppendEntriesResponse,
    ) : RaftEvent

    data class RequestVoteResponseReceived(
        val from: NodeId,
        val term: Term,
        val voteGranted: Boolean,
    ) : RaftEvent

    // Timer events
    data object ElectionTimeout : RaftEvent   // Figure 2: Followers / Candidates rule
    data object HeartbeatTick : RaftEvent     // Figure 2: Leaders rule (idle period)

    // Client interaction (Section 8)
    data class ClientSubmit<C>(
        val command: C,
        val reply: CompletableDeferred<SubmitResult<*>>,
    ) : RaftEvent

    // Apply loop feedback ("All Servers" rule: increment lastApplied)
    data class Applied(val through: LogIndex) : RaftEvent

    // Lifecycle
    data class Stop(val reply: CompletableDeferred<Unit>) : RaftEvent
}
```

The loop body:

```kotlin
for (event in mailbox) {
    // Figure 2 "All Servers" rule (term observation)
    event.observedTerm()?.let { incoming -> stepDownIfNewerTerm(incoming) }

    when (event) {
        is AppendEntriesReceived -> handleAppendEntries(event)
        is RequestVoteReceived -> handleRequestVote(event)
        is AppendEntriesResponseReceived -> handleAppendEntriesResponse(event)
        is RequestVoteResponseReceived -> handleRequestVoteResponse(event)
        ElectionTimeout -> startElection()
        HeartbeatTick -> sendHeartbeatsIfLeader()
        is ClientSubmit<*> -> handleClientSubmit(event)
        is Applied -> lastApplied = event.through
        is Stop -> {
            shutdown(); event.reply.complete(Unit); return
        }
    }

    // Figure 2 "All Servers" rule (apply commit)
    if (commitIndex > lastApplied) handOffToApplyLoop(lastApplied + 1, commitIndex)
}
```

Handlers consult the same persistent and volatile state listed in "Node State"
and call `StableStorage` as required by the next section.

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
  bool no_op = 4;
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

## Persistence and RPC Response Ordering

Figure 2 annotates the persistent state with: *"(Updated on stable storage
before responding to RPCs)"*. The Raft loop turns that single sentence into a
small number of explicit ordering rules. Every handler that mutates persistent
state must complete its `StableStorage` call **before** the matching reply or
side effect is released.

1. **Granting a vote.** When `handleRequestVote` decides to grant a vote, it
   updates `currentTerm` / `votedFor`, calls
   `StableStorage.saveTermAndVote(currentTerm, votedFor)`, and only then
   completes the inbound `reply` deferred with `voteGranted = true`.
2. **Appending new entries from a leader.** When `handleAppendEntries` accepts
   entries from the leader, it appends them to the in-memory log, calls
   `StableStorage.appendEntries(newEntries)`, and only then completes the reply
   with `success = true`.
3. **Truncating a conflicting suffix.** Truncation
   (`StableStorage.truncateSuffix(fromIndex)`) must complete before any
   subsequent append or response that observes the truncated tail.
4. **Stepping down on a higher term.** When the loop observes
   `incomingTerm > currentTerm`, it updates `currentTerm`, clears `votedFor`,
   calls `StableStorage.saveTermAndVote(...)`, and only then runs the rest of
   the event handler. Any reply that already includes `term = currentTerm` must
   reflect the persisted value.
5. **Leader accepting a client command.** When `handleClientSubmit` decides
   this node is leader, it appends the new entry to its log, calls
   `StableStorage.appendEntries([entry])`, and only then dispatches
   `AppendEntries` to peers via the Replicators (Section 5.3:
   *"The leader appends the command to its log as a new entry, then issues
   AppendEntries RPCs in parallel..."*).

These ordering rules are restated as invariants in "Safety Invariants" below
and are the primary correctness obligation of the Raft loop. Tests must verify
each one independently.

## Execution Model

`start()` launches the coroutines described in "Server Implementation
Structure":

- The **Raft loop** itself.
- The **apply loop** drained by the state machine.
- The **election timer** and **heartbeat ticker** that translate `delay(…)`
  into mailbox events.
- Per-peer **Replicator** coroutines, started and cancelled by the Raft loop as
  it enters and leaves leader state.

`stop()` posts a `Stop` event, waits for the Raft loop to drain, cancels timers
and Replicators, completes any remaining `submit` waiters with `Unavailable`,
and finally cancels the apply loop.

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
- Launch one `Replicator(peerId)` coroutine per peer (see "Server Implementation
  Structure"). The Replicators own the actual `AppendEntries` send loop; the
  Raft loop only updates `nextIndex` / `matchIndex` / `commitIndex` when
  responses come back via `AppendEntriesResponseReceived`.
- Have the Replicators send an immediate empty `AppendEntries` (heartbeat).
- Append and replicate a no-op entry for the new term. This helps the leader
  discover committed entries from previous terms and is required before
  optimized read-only requests are added. In v1 this is marked with
  `LogEntry.no_op`; command bytes are empty on the wire and the typed log stores
  `command = null`, so the apply loop skips state-machine execution for that
  index.

On stepping down (a higher term observed, or losing leadership through any
other path), the Raft loop cancels all Replicators before processing further
events. No `AppendEntries` may be in-flight from a node that is no longer leader
by the time it accepts a new `RequestVote`.

## Log Replication

The leader handles `submit(command)` by:

1. Appending a new log entry with the current term to the in-memory log.
2. Calling `StableStorage.appendEntries([entry])` and waiting for it to return
   before any replication RPC is dispatched. This is ordering rule 5 in
   "Persistence and RPC Response Ordering".
3. Notifying the Replicators that there is new work; each peer's coroutine then
   dispatches `AppendEntries` starting from its `nextIndex`.
4. Registering the caller's `CompletableDeferred` in `pendingSubmits[index]`.
5. Completing the submit call when the apply loop reports the entry as applied
   (see "Applying Entries").

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

Apply runs in the dedicated apply-loop coroutine (see "Server Implementation
Structure"), not inside the Raft loop. Whenever the Raft loop observes
`commitIndex > lastApplied` at the end of processing an event, it sends the
range `(lastApplied + 1 .. commitIndex)` to the apply-loop channel together
with a snapshot of the relevant log entries.

For each entry in the range, the apply loop, in order:

1. Calls `stateMachine.apply(decode(entry.command))`.
2. Completes any `pendingSubmits[index]` deferred with
   `SubmitResult.Applied(index, term, result)`.
3. Posts `Applied(through = index)` back into the Raft loop mailbox, which
   bumps `lastApplied` on the next iteration.

Posting `Applied` from the apply loop is how the Raft loop's `lastApplied`
field stays consistent with the state machine's view of the world. Because all
mutations of `lastApplied` flow through the same mailbox, the field never goes
backwards or skips an index.

If a command originated on a previous leader, applying it still updates the
state machine but may not have a local submit waiter; `pendingSubmits.remove`
returns `null` in that case and is ignored.

If `apply` fails, the node should stop and surface `Unavailable`, because Raft
cannot safely skip a committed command. Application-level retry policy belongs
inside the state machine.

## Client Semantics

Clients should submit all commands to the leader. A non-leader response includes
the most recently known `leaderId` when available.

`submit(command)` is `suspend` and stays suspended until the command is both
committed and applied. This is the implementation of Raft Section 8
("Client interaction"): *"a Raft-based RPC system should not return a result to
the client until that command has been committed and applied to the state
machine."* The mechanics:

- The leader's `handleClientSubmit` registers a `CompletableDeferred` in
  `pendingSubmits[index]` once the entry is appended to the log (and persisted,
  per ordering rule 5).
- The apply loop resolves that deferred when it finishes calling
  `stateMachine.apply(...)`.
- `submit` returns the resolved `SubmitResult.Applied(index, term, result)`.

Callers that want to pipeline multiple commands do not need a separate async
API; they wrap each call in `async { node.submit(c) }`. Each submission
independently waits for its own commit-and-apply, so linearizability is
preserved within each command's `Deferred`.

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

### Persistence ordering invariants

Restating the rules from "Persistence and RPC Response Ordering" as
invariants the tests must check:

1. **Vote-grant ordering.** No `RequestVoteResponse` with `voteGranted = true`
   is sent before `StableStorage.saveTermAndVote(currentTerm, votedFor)`
   completes for that decision.
2. **Append-success ordering.** No `AppendEntriesResponse` with
   `success = true` is sent before `StableStorage.appendEntries(...)`
   completes for the entries this response acknowledges.
3. **Truncation ordering.** No reply observes a truncated tail before
   `StableStorage.truncateSuffix(...)` completes for that truncation.
4. **Higher-term ordering.** Any RPC response carrying `term = currentTerm`
   reflects a value already persisted via `saveTermAndVote`.
5. **Leader-replication ordering.** No `AppendEntries` containing a new client
   entry is sent before `StableStorage.appendEntries([entry])` completes for
   that entry.

## Testing Strategy

Use `InMemoryStableStorage`, `InMemoryRaftService`, and a deterministic
`RaftScheduler` for unit and simulation tests. Each layer of the implementation
maps to a distinct test surface:

- **Raft loop handlers** — single-node behavior of each Figure 2 rule.
  Construct a Raft loop, post a single `RaftEvent`, then inspect resulting
  state, `StableStorage` calls, and outbound RPCs captured by the in-memory
  transport.
- **Raft loop + Replicators + apply loop** — single-node end-to-end. Drive
  timers through the deterministic scheduler; verify `pendingSubmits` deferreds
  complete in apply order.
- **Multi-node simulation** — election, replication, partition, recovery. Wire
  several Raft loops together with a single `InMemoryNetwork` that can inject
  drops, delays, and reorderings under a single virtual-time scheduler.
- **`kotlinx.rpc` adapter** — wire round-trip. Stand up a `RaftService`
  server/client pair on the in-memory `kotlinx.rpc` transport; confirm Proto
  messages map correctly to `RaftEvent` arrivals.

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

## Appendix: Reference Implementations

Other open-source Raft implementations informed several of the choices above,
but this design intentionally does not import their vocabulary or public types.
The paper is the primary reference; the libraries below are background.

- **etcd-io/raft** (Go). A pure state machine driven externally by a
  `Ready` / `Advance` loop. We borrow its discipline around persistence
  ordering, but not its API. `Ready` is not a concept in the paper; this
  implementation expresses the same invariants through per-handler ordering
  rules in "Persistence and RPC Response Ordering".
- **hashicorp/raft** (Go). Owns its own goroutines and channels much like the
  Raft loop / Replicators / apply loop structure here. Our split into a single
  loop plus per-peer Replicators and a dedicated apply loop is closest to this
  shape, but the public API does not adopt their `ApplyFuture` type.
- **tikv/raft-rs** (Rust). The pure-core half of etcd's design, ported to Rust.
  Same comments as for etcd-io/raft.

When in doubt, prefer the paper's wording over any term used in these
libraries. New abstractions should be added only when they correspond to
something the paper already names.
