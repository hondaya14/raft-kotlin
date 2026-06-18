package co.hondaya.raft.loop

import co.hondaya.raft.cluster.*
import co.hondaya.raft.command.*
import co.hondaya.raft.node.*
import co.hondaya.raft.log.*
import co.hondaya.raft.scheduler.DefaultRaftScheduler
import co.hondaya.raft.scheduler.RaftScheduler
import co.hondaya.raft.storage.StableStorage
import co.hondaya.raft.transport.*
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration.Companion.seconds

class CoroutineRaftNode<C : Any, R : Any>(
    private val config: ClusterConfig,
    private val storage: StableStorage<C>,
    private val service: RaftService,
    private val codec: CommandCodec<C>,
    private val stateMachine: StateMachine<C, R>,
    private val scheduler: RaftScheduler = DefaultRaftScheduler(),
    coroutineContext: CoroutineContext = Dispatchers.Default,
) : RaftNode<C, R>, RaftPeerEndpoint {
    override val id: NodeId = config.selfId

    private val scope = CoroutineScope(SupervisorJob() + coroutineContext)
    private val raftEventQueue = Channel<RaftEvent<C, R>>(Channel.UNLIMITED)
    private val committedEntryQueue = Channel<CommittedEntries<C>>(Channel.UNLIMITED)
    private val pendingSubmits = linkedMapOf<LogIndex, CompletableDeferred<SubmitResult<R>>>()

    private var raftEventLoopJob: Job? = null
    private var stateMachineApplyJob: Job? = null
    private var electionTimerJob: Job? = null
    private var heartbeatJob: Job? = null
    private val replicatorJobs = linkedMapOf<NodeId, Job>()
    private val replicatorMailboxes = linkedMapOf<NodeId, Channel<AppendEntriesRequest>>()
    private var started = false

    private var state = NodeState.FOLLOWER
    private var currentTerm = Term(0)
    private var votedFor: NodeId? = null
    private val log = mutableListOf<LogEntry<C>>()
    private var commitIndex = LogIndex(0)
    private var lastApplied = LogIndex(0)
    private var applyQueuedThrough = LogIndex(0)
    private var leaderId: NodeId? = null
    private val nextIndex = linkedMapOf<NodeId, LogIndex>()
    private val matchIndex = linkedMapOf<NodeId, LogIndex>()
    private val votesGranted = linkedSetOf<NodeId>()
    private var statusSnapshot = snapshotStatus()

    override suspend fun start() {
        if (started) return
        val persistentState = storage.load()
        currentTerm = persistentState.currentTerm
        votedFor = persistentState.votedFor
        log.clear()
        log += persistentState.log.sortedBy { it.index.value }
        statusSnapshot = snapshotStatus()
        started = true
        stateMachineApplyJob = scope.launch { runStateMachineApplyLoop() }
        raftEventLoopJob = scope.launch { runRaftEventLoop() }
        resetElectionTimer()
    }

    override suspend fun stop() {
        if (!started) return
        val reply = CompletableDeferred<Unit>()
        raftEventQueue.send(RaftEvent.Stop(reply))
        reply.await()
    }

    override suspend fun submit(command: C): SubmitResult<R> {
        if (!started) return SubmitResult.Unavailable("node is not started")
        val reply = CompletableDeferred<SubmitResult<R>>()
        raftEventQueue.send(RaftEvent.ClientSubmit(command, reply))
        return reply.await()
    }

    override fun status(): RaftStatus = statusSnapshot

    override suspend fun requestVote(request: RequestVoteRequest): RequestVoteResponse {
        val reply = CompletableDeferred<RequestVoteResponse>()
        raftEventQueue.send(RaftEvent.RequestVoteReceived(request, reply))
        return reply.await()
    }

    override suspend fun appendEntries(request: AppendEntriesRequest): AppendEntriesResponse {
        val reply = CompletableDeferred<AppendEntriesResponse>()
        raftEventQueue.send(RaftEvent.AppendEntriesReceived(request, reply))
        return reply.await()
    }

    private suspend fun runRaftEventLoop() {
        for (event in raftEventQueue) {
            observeTerm(event.observedTerm())
            when (event) {
                is RaftEvent.AppendEntriesReceived -> handleAppendEntries(event)
                is RaftEvent.RequestVoteReceived -> handleRequestVote(event)
                is RaftEvent.AppendEntriesResponseReceived -> handleAppendEntriesResponse(event)
                is RaftEvent.RequestVoteResponseReceived -> handleRequestVoteResponse(event)
                is RaftEvent.ElectionTimeout -> startElection()
                is RaftEvent.HeartbeatTick -> sendHeartbeatsIfLeader()
                is RaftEvent.ClientSubmit<*, *> -> handleClientSubmit(event)
                is RaftEvent.Applied<*> -> {
                    if (event.appliedThrough > lastApplied) {
                        lastApplied = event.appliedThrough
                    }
                    if (event.applyResult != null) {
                        pendingSubmits.remove(event.appliedThrough)?.complete(
                            SubmitResult.Applied(event.appliedThrough, event.appliedTerm, event.applyResult as R),
                        )
                    }
                }

                is RaftEvent.ApplyFailed -> {
                    pendingSubmits.values.forEach {
                        it.complete(SubmitResult.Unavailable(event.failureReason))
                    }
                    pendingSubmits.clear()
                }

                is RaftEvent.Stop -> {
                    shutdown()
                    event.stopped.complete(Unit)
                    return
                }
            }
            enqueueCommittedEntriesForApply()
            statusSnapshot = snapshotStatus()
        }
    }

    private suspend fun observeTerm(term: Term?) {
        if (term != null && term > currentTerm) {
            stepDown(term)
        }
    }

    private suspend fun handleRequestVote(event: RaftEvent.RequestVoteReceived) {
        val request = event.rpcRequest
        if (request.term < currentTerm) {
            event.rpcReply.complete(RequestVoteResponse(currentTerm, false))
            return
        }

        val canVoteForCandidate = votedFor == null || votedFor == request.candidateId
        val grantVote = canVoteForCandidate && isCandidateLogUpToDate(request.lastLogIndex, request.lastLogTerm)
        if (grantVote) {
            state = NodeState.FOLLOWER
            leaderId = null
            votedFor = request.candidateId
            storage.saveTermAndVote(currentTerm, votedFor)
            resetElectionTimer()
        }
        event.rpcReply.complete(RequestVoteResponse(currentTerm, grantVote))
    }

    private suspend fun handleAppendEntries(event: RaftEvent.AppendEntriesReceived) {
        val request = event.rpcRequest
        if (request.term < currentTerm) {
            event.rpcReply.complete(AppendEntriesResponse(currentTerm, false))
            return
        }

        state = NodeState.FOLLOWER
        leaderId = request.leaderId
        resetElectionTimer()

        if (!hasLogEntry(request.prevLogIndex, request.prevLogTerm)) {
            event.rpcReply.complete(AppendEntriesResponse(currentTerm, false))
            return
        }

        val decodedEntries = request.entries.map {
            val command = if (it.noOp) null else codec.decode(it.command)
            LogEntry(it.index, it.term, command, it.noOp)
        }
        reconcileFollowerLog(decodedEntries)
        if (request.leaderCommit > commitIndex) {
            commitIndex = minOf(request.leaderCommit, lastLogIndex())
        }
        event.rpcReply.complete(AppendEntriesResponse(currentTerm, true))
    }

    private suspend fun reconcileFollowerLog(entries: List<LogEntry<C>>) {
        if (entries.isEmpty()) return

        val firstConflict = entries.firstOrNull { incoming ->
            logEntryAt(incoming.index)?.term != incoming.term
        }
        if (firstConflict != null) {
            storage.truncateSuffix(firstConflict.index)
            while (log.isNotEmpty() && log.last().index >= firstConflict.index) {
                log.removeAt(log.lastIndex)
            }
        }

        val newEntries = entries.filter { it.index > lastLogIndex() }
        if (newEntries.isNotEmpty()) {
            log += newEntries
            storage.appendEntries(newEntries)
        }
    }

    private suspend fun startElection() {
        if (state == NodeState.LEADER) return
        state = NodeState.CANDIDATE
        leaderId = null
        currentTerm += 1
        votedFor = id
        votesGranted.clear()
        votesGranted += id
        storage.saveTermAndVote(currentTerm, votedFor)
        resetElectionTimer()

        if (hasMajority(votesGranted.size)) {
            becomeLeader()
            return
        }

        val request = RequestVoteRequest(currentTerm, id, lastLogIndex(), lastLogTerm())
        config.peers.forEach { peer ->
            scope.launch {
                val response = withTimeoutOrNull(1.seconds) {
                    service.requestVote(peer, request)
                } ?: RequestVoteResponse(request.term, false)
                raftEventQueue.send(RaftEvent.RequestVoteResponseReceived(peer, response.term, response.voteGranted))
            }
        }
    }

    private suspend fun handleRequestVoteResponse(event: RaftEvent.RequestVoteResponseReceived) {
        if (state != NodeState.CANDIDATE || event.responseTerm != currentTerm) return
        if (event.grantedVote) {
            votesGranted += event.respondedBy
            if (hasMajority(votesGranted.size)) {
                becomeLeader()
            }
        }
    }

    private suspend fun becomeLeader() {
        state = NodeState.LEADER
        leaderId = id
        votesGranted.clear()
        val next = lastLogIndex() + 1
        nextIndex.clear()
        matchIndex.clear()
        config.peers.forEach { peer ->
            nextIndex[peer] = next
            matchIndex[peer] = LogIndex(0)
        }
        startReplicators()
        appendLeaderNoOp()
        stopElectionTimer()
        startHeartbeatTicker()
        sendHeartbeatsIfLeader()
    }

    private suspend fun appendLeaderNoOp() {
        val entry = LogEntry<C>(lastLogIndex() + 1, currentTerm, null, noOp = true)
        log += entry
        storage.appendEntries(listOf(entry))
        if (hasMajority(1)) {
            commitIndex = entry.index
            enqueueCommittedEntriesForApply()
        }
    }

    private fun sendHeartbeatsIfLeader() {
        if (state != NodeState.LEADER) return
        config.peers.forEach { peer -> enqueueAppendEntries(peer) }
    }

    private suspend fun handleClientSubmit(event: RaftEvent.ClientSubmit<*, *>) {
        if (state != NodeState.LEADER) {
            (event.submitResult as CompletableDeferred<SubmitResult<R>>).complete(SubmitResult.NotLeader(leaderId))
            return
        }

        val command = event.submittedCommand as C
        val entry = LogEntry(lastLogIndex() + 1, currentTerm, command)
        log += entry
        storage.appendEntries(listOf(entry))
        val reply = event.submitResult as CompletableDeferred<SubmitResult<R>>
        pendingSubmits[entry.index] = reply

        if (hasMajority(1)) {
            commitIndex = entry.index
            enqueueCommittedEntriesForApply()
        } else {
            config.peers.forEach { peer -> enqueueAppendEntries(peer) }
        }
    }

    private suspend fun handleAppendEntriesResponse(event: RaftEvent.AppendEntriesResponseReceived) {
        if (state != NodeState.LEADER || event.sentRequest.term != currentTerm || event.rpcResponse.term != currentTerm) {
            return
        }
        val peer = event.respondedBy
        if (event.rpcResponse.success) {
            val replicatedIndex = event.sentRequest.entries.lastOrNull()?.index ?: event.sentRequest.prevLogIndex
            matchIndex[peer] = replicatedIndex
            nextIndex[peer] = replicatedIndex + 1
            advanceCommitIndex()
        } else {
            val next = nextIndex.getValue(peer)
            nextIndex[peer] = if (next.value > 1) next - 1 else LogIndex(1)
            enqueueAppendEntries(peer)
        }
    }

    private fun advanceCommitIndex() {
        val last = lastLogIndex()
        var candidate = commitIndex + 1
        while (candidate <= last) {
            val entry = logEntryAt(candidate) ?: break
            if (entry.term == currentTerm && replicatedCount(candidate) >= config.majority) {
                commitIndex = candidate
            }
            candidate += 1
        }
    }

    private fun enqueueAppendEntries(peer: NodeId) {
        if (state != NodeState.LEADER) return
        val requests = replicatorMailboxes[peer] ?: return
        val next = nextIndex[peer] ?: (lastLogIndex() + 1)
        val prevIndex = if (next.value > 0) next - 1 else LogIndex(0)
        val entries = log
            .filter { it.index >= next }
            .map {
                val command = if (it.noOp) ByteArray(0) else codec.encode(requireNotNull(it.command))
                WireLogEntry(it.index, it.term, command, it.noOp)
            }
        val request = AppendEntriesRequest(
            term = currentTerm,
            leaderId = id,
            prevLogIndex = prevIndex,
            prevLogTerm = termAt(prevIndex),
            entries = entries,
            leaderCommit = commitIndex,
        )
        requests.trySend(request)
    }

    private suspend fun enqueueCommittedEntriesForApply() {
        if (commitIndex <= applyQueuedThrough) return
        val entries = log.filter { it.index > applyQueuedThrough && it.index <= commitIndex }
        if (entries.isNotEmpty()) {
            committedEntryQueue.send(CommittedEntries(entries))
            applyQueuedThrough = entries.last().index
        }
    }

    private suspend fun runStateMachineApplyLoop() {
        for (range in committedEntryQueue) {
            for (entry in range.entries) {
                try {
                    if (entry.noOp) {
                        raftEventQueue.send(RaftEvent.Applied<R>(entry.index, entry.term, null))
                    } else {
                        val result = stateMachine.apply(requireNotNull(entry.command))
                        raftEventQueue.send(RaftEvent.Applied(entry.index, entry.term, result))
                    }
                } catch (error: Throwable) {
                    raftEventQueue.send(
                        RaftEvent.ApplyFailed(
                            "state machine failed: ${error.message ?: error::class.simpleName}",
                        ),
                    )
                    raftEventQueue.send(RaftEvent.Stop(CompletableDeferred()))
                    return
                }
            }
        }
    }

    private suspend fun stepDown(newTerm: Term) {
        state = NodeState.FOLLOWER
        leaderId = null
        currentTerm = newTerm
        votedFor = null
        votesGranted.clear()
        nextIndex.clear()
        matchIndex.clear()
        stopReplicators()
        stopHeartbeatTicker()
        storage.saveTermAndVote(currentTerm, null)
        resetElectionTimer()
    }

    private suspend fun shutdown() {
        started = false
        stopElectionTimer()
        stopHeartbeatTicker()
        stopReplicators()
        pendingSubmits.values.forEach { it.complete(SubmitResult.Unavailable("node stopped")) }
        pendingSubmits.clear()
        raftEventQueue.close()
        committedEntryQueue.close()
        listOfNotNull(stateMachineApplyJob).joinAll()
        scope.cancel()
    }

    private fun resetElectionTimer() {
        stopElectionTimer()
        electionTimerJob = scope.launch {
            scheduler.delay(scheduler.nextElectionTimeout())
            raftEventQueue.send(RaftEvent.ElectionTimeout)
        }
    }

    private fun stopElectionTimer() {
        electionTimerJob?.cancel()
        electionTimerJob = null
    }

    private fun startHeartbeatTicker() {
        stopHeartbeatTicker()
        heartbeatJob = scope.launch {
            while (true) {
                scheduler.delay(config.heartbeatInterval)
                raftEventQueue.send(RaftEvent.HeartbeatTick)
            }
        }
    }

    private fun stopHeartbeatTicker() {
        heartbeatJob?.cancel()
        heartbeatJob = null
    }

    private fun startReplicators() {
        stopReplicators()
        config.peers.forEach { peer ->
            val requests = Channel<AppendEntriesRequest>(Channel.CONFLATED)
            replicatorMailboxes[peer] = requests
            replicatorJobs[peer] = scope.launch {
                for (request in requests) {
                    val response = withTimeoutOrNull(1.seconds) {
                        service.appendEntries(peer, request)
                    } ?: AppendEntriesResponse(request.term, false)
                    raftEventQueue.send(RaftEvent.AppendEntriesResponseReceived(peer, request, response))
                }
            }
        }
    }

    private fun stopReplicators() {
        replicatorMailboxes.values.forEach { it.close() }
        replicatorMailboxes.clear()
        replicatorJobs.values.forEach { it.cancel() }
        replicatorJobs.clear()
    }

    private fun isCandidateLogUpToDate(lastIndex: LogIndex, lastTerm: Term): Boolean =
        lastTerm > lastLogTerm() || (lastTerm == lastLogTerm() && lastIndex >= lastLogIndex())

    private fun hasLogEntry(index: LogIndex, term: Term): Boolean =
        index.value == 0L && term.value == 0L || termAt(index) == term

    private fun replicatedCount(index: LogIndex): Int =
        1 + matchIndex.values.count { it >= index }

    private fun hasMajority(count: Int): Boolean = count >= config.majority

    private fun lastLogIndex(): LogIndex = log.lastOrNull()?.index ?: LogIndex(0)

    private fun lastLogTerm(): Term = log.lastOrNull()?.term ?: Term(0)

    private fun termAt(index: LogIndex): Term =
        if (index.value == 0L) Term(0) else logEntryAt(index)?.term ?: Term(0)

    private fun logEntryAt(index: LogIndex): LogEntry<C>? =
        log.firstOrNull { it.index == index }

    private fun snapshotStatus(): RaftStatus = RaftStatus(
        id = id,
        state = state,
        currentTerm = currentTerm,
        votedFor = votedFor,
        leaderId = leaderId,
        commitIndex = commitIndex,
        lastApplied = lastApplied,
        lastLogIndex = lastLogIndex(),
        lastLogTerm = lastLogTerm(),
    )
}

private data class CommittedEntries<C : Any>(
    val entries: List<LogEntry<C>>,
)
