package co.hondaya.raft

import co.hondaya.raft.loop.CoroutineRaftNode
import co.hondaya.raft.scheduler.RaftScheduler
import co.hondaya.raft.storage.InMemoryStableStorage
import co.hondaya.raft.storage.PersistentState
import co.hondaya.raft.transport.AppendEntriesRequest
import co.hondaya.raft.transport.AppendEntriesResponse
import co.hondaya.raft.transport.InMemoryTransport
import co.hondaya.raft.transport.RaftService
import co.hondaya.raft.transport.RequestVoteRequest
import co.hondaya.raft.transport.RequestVoteResponse
import co.hondaya.raft.transport.WireLogEntry
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.coroutines.CoroutineContext
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

class CoroutineRaftNodeTest {
    @Test
    fun requestVoteGrantsOnlyAfterPersistingVote() = runBlocking {
        val storage = InMemoryStableStorage<String>()
        val transport = InMemoryTransport()
        val node = newNode("a", emptySet(), storage, transport, NeverElectionScheduler)

        node.start()
        val response = node.requestVote(
            RequestVoteRequest(
                term = Term(1),
                candidateId = NodeId("b"),
                lastLogIndex = LogIndex(0),
                lastLogTerm = Term(0),
            ),
        )

        assertTrue(response.voteGranted)
        assertEquals(Term(1), response.term)
        assertEquals(Term(1), storage.load().currentTerm)
        assertEquals(NodeId("b"), storage.load().votedFor)

        node.stop()
    }

    @Test
    fun appendEntriesPersistsAndAppliesCommittedEntry() = runBlocking {
        val storage = InMemoryStableStorage<String>()
        val stateMachine = RecordingStateMachine()
        val transport = InMemoryTransport()
        val node = newNode("a", emptySet(), storage, transport, NeverElectionScheduler, stateMachine)

        node.start()
        val response = node.appendEntries(
            AppendEntriesRequest(
                term = Term(1),
                leaderId = NodeId("leader"),
                prevLogIndex = LogIndex(0),
                prevLogTerm = Term(0),
                entries = listOf(WireLogEntry(LogIndex(1), Term(1), "set-x".encodeToByteArray())),
                leaderCommit = LogIndex(1),
            ),
        )

        assertTrue(response.success)
        withTimeout(500.milliseconds) {
            while (stateMachine.applied != listOf("set-x") || node.status().lastApplied < LogIndex(1)) delay(10)
        }
        assertEquals(listOf(LogEntry(LogIndex(1), Term(1), "set-x")), storage.load().log)
        assertEquals(LogIndex(1), node.status().commitIndex)
        assertEquals(LogIndex(1), node.status().lastApplied)

        node.stop()
    }

    @Test
    fun duplicateAppendEntriesAppliesCommandOnlyOnce() = runBlocking {
        val storage = InMemoryStableStorage<String>()
        val stateMachine = RecordingStateMachine()
        val node = newNode("a", emptySet(), storage, InMemoryTransport(), NeverElectionScheduler, stateMachine)
        val request = AppendEntriesRequest(
            term = Term(1),
            leaderId = NodeId("leader"),
            prevLogIndex = LogIndex(0),
            prevLogTerm = Term(0),
            entries = listOf(WireLogEntry(LogIndex(1), Term(1), "cmd".encodeToByteArray())),
            leaderCommit = LogIndex(1),
        )

        node.start()
        assertTrue(node.appendEntries(request).success)
        assertTrue(node.appendEntries(request).success)
        withTimeout(500.milliseconds) {
            while (node.status().lastApplied < LogIndex(1)) delay(10)
        }

        assertEquals(listOf("cmd"), stateMachine.applied)
        assertEquals(listOf(LogEntry(LogIndex(1), Term(1), "cmd")), storage.load().log)

        node.stop()
    }

    @Test
    fun requestVoteRejectsOlderTerm() = runBlocking {
        val storage = InMemoryStableStorage(PersistentState<String>(currentTerm = Term(3)))
        val node = newNode("a", emptySet(), storage, InMemoryTransport(), NeverElectionScheduler)

        node.start()
        val response = node.requestVote(
            RequestVoteRequest(
                term = Term(2),
                candidateId = NodeId("candidate"),
                lastLogIndex = LogIndex(0),
                lastLogTerm = Term(0),
            ),
        )

        assertEquals(RequestVoteResponse(Term(3), voteGranted = false), response)
        assertEquals(Term(3), storage.load().currentTerm)
        assertNull(storage.load().votedFor)

        node.stop()
    }

    @Test
    fun appendEntriesRejectsOlderTerm() = runBlocking {
        val storage = InMemoryStableStorage(PersistentState(currentTerm = Term(3), log = listOf(LogEntry(LogIndex(1), Term(3), "local"))))
        val node = newNode("a", emptySet(), storage, InMemoryTransport(), NeverElectionScheduler)

        node.start()
        val response = node.appendEntries(
            AppendEntriesRequest(
                term = Term(2),
                leaderId = NodeId("leader"),
                prevLogIndex = LogIndex(1),
                prevLogTerm = Term(3),
                entries = emptyList(),
                leaderCommit = LogIndex(0),
            ),
        )

        assertEquals(AppendEntriesResponse(Term(3), success = false), response)
        assertEquals(listOf(LogEntry(LogIndex(1), Term(3), "local")), storage.load().log)

        node.stop()
    }

    @Test
    fun appendEntriesRejectsPrevLogMismatch() = runBlocking {
        val storage = InMemoryStableStorage(PersistentState(log = listOf(LogEntry(LogIndex(1), Term(1), "local"))))
        val node = newNode("a", emptySet(), storage, InMemoryTransport(), NeverElectionScheduler)

        node.start()
        val response = node.appendEntries(
            AppendEntriesRequest(
                term = Term(1),
                leaderId = NodeId("leader"),
                prevLogIndex = LogIndex(1),
                prevLogTerm = Term(99),
                entries = listOf(WireLogEntry(LogIndex(2), Term(1), "new".encodeToByteArray())),
                leaderCommit = LogIndex(0),
            ),
        )

        assertEquals(AppendEntriesResponse(Term(1), success = false), response)
        assertEquals(listOf(LogEntry(LogIndex(1), Term(1), "local")), storage.load().log)

        node.stop()
    }

    @Test
    fun stateMachineFailureCompletesSubmitUnavailable() = runBlocking {
        val node = newNode(
            id = "a",
            peers = emptySet(),
            scheduler = FastElectionScheduler,
            stateMachine = FailingStateMachine,
        )

        node.start()
        withTimeout(500.milliseconds) {
            while (node.status().state != NodeState.LEADER) delay(10)
        }

        val result = node.submit("fail")

        val unavailable = assertIs<SubmitResult.Unavailable>(result)
        assertTrue(unavailable.reason.startsWith("state machine failed"))
    }

    @Test
    fun singleNodeElectsLeaderAndAppliesSubmittedCommand() = runBlocking {
        val node = newNode("a", emptySet(), scheduler = FastElectionScheduler)

        node.start()
        withTimeout(500.milliseconds) {
            while (node.status().state != NodeState.LEADER) delay(10)
        }

        val result = node.submit("cmd-1")

        val applied = assertIs<SubmitResult.Applied<String>>(result)
        assertEquals(LogIndex(2), applied.index)
        assertEquals("applied:cmd-1", applied.result)
        assertEquals(LogIndex(2), node.status().commitIndex)
        assertEquals(LogIndex(2), node.status().lastApplied)

        node.stop()
    }

    @Test
    @OptIn(ExperimentalCoroutinesApi::class)
    fun injectedCoroutineContextSupportsVirtualTimeElection() = runTest {
        val dispatcher = StandardTestDispatcher(testScheduler)
        val node = newNode(
            id = "a",
            peers = emptySet(),
            scheduler = FastElectionScheduler,
            coroutineContext = dispatcher,
        )

        node.start()
        advanceTimeBy(10)
        runCurrent()

        assertEquals(NodeState.LEADER, node.status().state)

        val stop = async { node.stop() }
        runCurrent()
        stop.await()
    }

    @Test
    fun leaderElectionAppendsAndSkipsNoOpEntry() = runBlocking {
        val storage = InMemoryStableStorage<String>()
        val stateMachine = RecordingStateMachine()
        val node = newNode(
            id = "a",
            peers = emptySet(),
            storage = storage,
            scheduler = FastElectionScheduler,
            stateMachine = stateMachine,
        )

        node.start()
        withTimeout(500.milliseconds) {
            while (node.status().lastApplied < LogIndex(1)) delay(10)
        }

        assertEquals(
            listOf(LogEntry<String>(LogIndex(1), Term(1), null, noOp = true)),
            storage.load().log,
        )
        assertEquals(emptyList(), stateMachine.applied)

        node.stop()
    }

    @Test
    fun leaderStepsDownAndPersistsHigherTermFromPeerResponse() = runBlocking {
        val storage = InMemoryStableStorage<String>()
        val service = HigherTermOnAppendService()
        val node = newNode(
            id = "a",
            peers = setOf(NodeId("b"), NodeId("c")),
            storage = storage,
            service = service,
            scheduler = OneShotElectionScheduler(),
        )

        node.start()
        withTimeout(1_000.milliseconds) {
            while (node.status().currentTerm < Term(2)) delay(10)
        }

        assertEquals(NodeState.FOLLOWER, node.status().state)
        assertEquals(Term(2), node.status().currentTerm)
        assertNull(node.status().votedFor)
        assertEquals(Term(2), storage.load().currentTerm)
        assertNull(storage.load().votedFor)

        node.stop()
    }

    @Test
    fun candidateStepsDownOnValidAppendEntriesFromSameTermLeader() = runBlocking {
        val storage = InMemoryStableStorage<String>()
        val node = newNode("a", emptySet(), storage, InMemoryTransport(), OneShotElectionScheduler())

        node.start()
        withTimeout(500.milliseconds) {
            while (node.status().state != NodeState.LEADER) delay(10)
        }
        val response = node.appendEntries(
            AppendEntriesRequest(
                term = node.status().currentTerm,
                leaderId = NodeId("leader"),
                prevLogIndex = node.status().lastLogIndex,
                prevLogTerm = node.status().lastLogTerm,
                entries = emptyList(),
                leaderCommit = node.status().commitIndex,
            ),
        )

        assertTrue(response.success)
        assertEquals(NodeState.FOLLOWER, node.status().state)
        assertEquals(NodeId("leader"), node.status().leaderId)

        node.stop()
    }

    @Test
    fun threeNodeClusterElectsLeaderAndReplicatesCommand() = runBlocking {
        val transport = InMemoryTransport()
        val nodes = listOf("a", "b", "c").associateWith { id ->
            newNode(
                id = id,
                peers = setOf("a", "b", "c").filterNot { it == id }.map(::NodeId).toSet(),
                service = transport,
                scheduler = if (id == "a") FastElectionScheduler else SlowElectionScheduler,
            )
        }
        nodes.forEach { (id, node) -> transport.register(NodeId(id), node) }
        nodes.values.forEach { it.start() }

        val leader = nodes.getValue("a")
        withTimeout(1_000.milliseconds) {
            while (leader.status().state != NodeState.LEADER) delay(10)
        }

        val result = leader.submit("cmd-1")

        val applied = assertIs<SubmitResult.Applied<String>>(result)
        assertEquals("applied:cmd-1", applied.result)
        withTimeout(1_000.milliseconds) {
            while (nodes.values.any { it.status().lastApplied < LogIndex(2) }) delay(10)
        }
        nodes.values.forEach {
            assertEquals(LogIndex(2), it.status().commitIndex)
            assertEquals(LogIndex(2), it.status().lastApplied)
        }

        nodes.values.forEach { it.stop() }
    }

    @Test
    fun restartedFollowerCatchesUpFromCurrentLeader() = runBlocking {
        val transport = InMemoryTransport()
        val followerStorage = InMemoryStableStorage<String>()
        val leader = newNode(
            id = "a",
            peers = setOf(NodeId("b"), NodeId("c")),
            service = transport,
            scheduler = FastElectionScheduler,
        )
        val stoppedFollower = newNode(
            id = "b",
            peers = setOf(NodeId("a"), NodeId("c")),
            storage = followerStorage,
            service = transport,
            scheduler = SlowElectionScheduler,
        )
        val otherFollower = newNode(
            id = "c",
            peers = setOf(NodeId("a"), NodeId("b")),
            service = transport,
            scheduler = SlowElectionScheduler,
        )

        transport.register(NodeId("a"), leader)
        transport.register(NodeId("b"), stoppedFollower)
        transport.register(NodeId("c"), otherFollower)
        listOf(leader, stoppedFollower, otherFollower).forEach { it.start() }
        withTimeout(1_000.milliseconds) {
            while (leader.status().state != NodeState.LEADER) delay(10)
        }

        stoppedFollower.stop()
        transport.unregister(NodeId("b"))

        val result = leader.submit("cmd-before-restart")
        assertIs<SubmitResult.Applied<String>>(result)

        val restartedFollower = newNode(
            id = "b",
            peers = setOf(NodeId("a"), NodeId("c")),
            storage = followerStorage,
            service = transport,
            scheduler = SlowElectionScheduler,
        )
        transport.register(NodeId("b"), restartedFollower)
        restartedFollower.start()

        withTimeout(1_000.milliseconds) {
            while (restartedFollower.status().lastApplied < LogIndex(2)) delay(10)
        }
        assertEquals(LogIndex(2), restartedFollower.status().commitIndex)
        assertEquals(
            listOf(
                LogEntry<String>(LogIndex(1), Term(1), null, noOp = true),
                LogEntry(LogIndex(2), Term(1), "cmd-before-restart"),
            ),
            followerStorage.load().log,
        )

        listOf(leader, restartedFollower, otherFollower).forEach { it.stop() }
    }

    @Test
    fun minorityPartitionCannotCommitNewEntry() = runBlocking {
        val transport = InMemoryTransport()
        val nodes = listOf("a", "b", "c").associateWith { id ->
            newNode(
                id = id,
                peers = setOf("a", "b", "c").filterNot { it == id }.map(::NodeId).toSet(),
                service = transport,
                scheduler = if (id == "a") FastElectionScheduler else SlowElectionScheduler,
            )
        }
        nodes.forEach { (id, node) -> transport.register(NodeId(id), node) }
        nodes.values.forEach { it.start() }
        val leader = nodes.getValue("a")
        withTimeout(1_000.milliseconds) {
            while (leader.status().state != NodeState.LEADER || leader.status().lastApplied < LogIndex(1)) {
                delay(10)
            }
        }

        transport.disconnect(NodeId("a"), NodeId("b"))
        transport.disconnect(NodeId("a"), NodeId("c"))

        val result = withTimeoutOrNull(150.milliseconds) {
            leader.submit("partitioned")
        }

        assertNull(result)
        assertEquals(LogIndex(1), leader.status().commitIndex)
        assertEquals(LogIndex(1), leader.status().lastApplied)

        nodes.values.forEach { it.stop() }
    }

    @Test
    fun leaderCrashAndNewElectionPreservesCommittedCommands() = runBlocking {
        val transport = InMemoryTransport()
        val storageById = listOf("a", "b", "c").associateWith { InMemoryStableStorage<String>() }
        val leader = newNode(
            id = "a",
            peers = setOf(NodeId("b"), NodeId("c")),
            storage = storageById.getValue("a"),
            service = transport,
            scheduler = FastElectionScheduler,
        )
        val nextLeader = newNode(
            id = "b",
            peers = setOf(NodeId("a"), NodeId("c")),
            storage = storageById.getValue("b"),
            service = transport,
            scheduler = MediumElectionScheduler,
        )
        val follower = newNode(
            id = "c",
            peers = setOf(NodeId("a"), NodeId("b")),
            storage = storageById.getValue("c"),
            service = transport,
            scheduler = SlowElectionScheduler,
        )
        mapOf("a" to leader, "b" to nextLeader, "c" to follower).forEach { (id, node) ->
            transport.register(NodeId(id), node)
            node.start()
        }
        withTimeout(1_000.milliseconds) {
            while (leader.status().state != NodeState.LEADER) delay(10)
        }

        assertIs<SubmitResult.Applied<String>>(leader.submit("committed-before-crash"))
        withTimeout(1_000.milliseconds) {
            while (listOf(leader, nextLeader, follower).any { it.status().lastApplied < LogIndex(2) }) delay(10)
        }

        leader.stop()
        transport.unregister(NodeId("a"))

        withTimeout(1_000.milliseconds) {
            while (nextLeader.status().state != NodeState.LEADER) delay(10)
        }
        assertIs<SubmitResult.Applied<String>>(nextLeader.submit("committed-after-crash"))
        withTimeout(1_000.milliseconds) {
            while (listOf(nextLeader, follower).any { it.status().lastApplied < LogIndex(4) }) delay(10)
        }

        listOf(nextLeader, follower).forEach {
            assertEquals(LogIndex(4), it.status().commitIndex)
            assertEquals(LogIndex(4), it.status().lastApplied)
        }
        assertEquals(
            listOf(
                LogEntry<String>(LogIndex(1), Term(1), null, noOp = true),
                LogEntry(LogIndex(2), Term(1), "committed-before-crash"),
                LogEntry<String>(LogIndex(3), Term(2), null, noOp = true),
                LogEntry(LogIndex(4), Term(2), "committed-after-crash"),
            ),
            storageById.getValue("b").load().log,
        )

        listOf(nextLeader, follower).forEach { it.stop() }
    }

    @Test
    fun fiveNodeClusterToleratesTwoStoppedNodes() = runBlocking {
        val transport = InMemoryTransport()
        val ids = listOf("a", "b", "c", "d", "e")
        val nodes = ids.associateWith { id ->
            newNode(
                id = id,
                peers = ids.filterNot { it == id }.map(::NodeId).toSet(),
                service = transport,
                scheduler = when (id) {
                    "a" -> FastElectionScheduler
                    "b" -> MediumElectionScheduler
                    else -> SlowElectionScheduler
                },
            )
        }
        nodes.forEach { (id, node) ->
            transport.register(NodeId(id), node)
            node.start()
        }
        val leader = nodes.getValue("a")
        withTimeout(1_000.milliseconds) {
            while (leader.status().state != NodeState.LEADER) delay(10)
        }

        nodes.getValue("d").stop()
        nodes.getValue("e").stop()
        transport.unregister(NodeId("d"))
        transport.unregister(NodeId("e"))

        val result = leader.submit("survive-two-stopped")

        assertIs<SubmitResult.Applied<String>>(result)
        withTimeout(1_000.milliseconds) {
            while (listOf("a", "b", "c").any { nodes.getValue(it).status().lastApplied < LogIndex(2) }) delay(10)
        }
        listOf("a", "b", "c").forEach { id ->
            assertEquals(LogIndex(2), nodes.getValue(id).status().commitIndex)
            assertEquals(LogIndex(2), nodes.getValue(id).status().lastApplied)
        }

        listOf("a", "b", "c").forEach { nodes.getValue(it).stop() }
    }

    @Test
    fun followerSubmitReturnsKnownLeader() = runBlocking {
        val node = newNode("a", emptySet(), scheduler = NeverElectionScheduler)
        node.start()

        val heartbeat = AppendEntriesRequest(
            term = Term(1),
            leaderId = NodeId("leader"),
            prevLogIndex = LogIndex(0),
            prevLogTerm = Term(0),
            entries = emptyList(),
            leaderCommit = LogIndex(0),
        )
        assertTrue(node.appendEntries(heartbeat).success)

        val result = node.submit("cmd")

        val notLeader = assertIs<SubmitResult.NotLeader>(result)
        assertEquals(NodeId("leader"), notLeader.leaderId)

        node.stop()
    }

    @Test
    fun staleCandidateLogIsRejected() = runBlocking {
        val storage = InMemoryStableStorage(
            PersistentState(
                currentTerm = Term(2),
                log = listOf(LogEntry(LogIndex(1), Term(2), "local")),
            ),
        )
        val node = newNode("a", emptySet(), storage, InMemoryTransport(), NeverElectionScheduler)
        node.start()

        val response = node.requestVote(
            RequestVoteRequest(
                term = Term(2),
                candidateId = NodeId("candidate"),
                lastLogIndex = LogIndex(1),
                lastLogTerm = Term(1),
            ),
        )

        assertEquals(Term(2), response.term)
        assertNull(storage.load().votedFor)
        assertTrue(!response.voteGranted)

        node.stop()
    }

    @Test
    fun appendEntriesTruncatesConflictingSuffixAndAppendsReplacement() = runBlocking {
        val storage = InMemoryStableStorage(
            PersistentState(
                currentTerm = Term(2),
                log = listOf(
                    LogEntry(LogIndex(1), Term(1), "stable"),
                    LogEntry(LogIndex(2), Term(2), "old"),
                    LogEntry(LogIndex(3), Term(2), "old-tail"),
                ),
            ),
        )
        val node = newNode("a", emptySet(), storage, InMemoryTransport(), NeverElectionScheduler)
        node.start()

        val response = node.appendEntries(
            AppendEntriesRequest(
                term = Term(3),
                leaderId = NodeId("leader"),
                prevLogIndex = LogIndex(1),
                prevLogTerm = Term(1),
                entries = listOf(
                    WireLogEntry(LogIndex(2), Term(3), "new".encodeToByteArray()),
                    WireLogEntry(LogIndex(3), Term(3), "new-tail".encodeToByteArray()),
                ),
                leaderCommit = LogIndex(0),
            ),
        )

        assertTrue(response.success)
        assertEquals(
            listOf(
                LogEntry(LogIndex(1), Term(1), "stable"),
                LogEntry(LogIndex(2), Term(3), "new"),
                LogEntry(LogIndex(3), Term(3), "new-tail"),
            ),
            storage.load().log,
        )
        assertEquals(LogIndex(3), node.status().lastLogIndex)
        assertEquals(Term(3), node.status().lastLogTerm)

        node.stop()
    }

    @Test
    fun restartedNodeReloadsPersistentTermVoteAndLog() = runBlocking {
        val storage = InMemoryStableStorage(
            PersistentState(
                currentTerm = Term(4),
                votedFor = NodeId("candidate-a"),
                log = listOf(LogEntry(LogIndex(1), Term(3), "existing")),
            ),
        )
        val node = newNode("a", emptySet(), storage, InMemoryTransport(), NeverElectionScheduler)

        node.start()

        assertEquals(Term(4), node.status().currentTerm)
        assertEquals(NodeId("candidate-a"), node.status().votedFor)
        assertEquals(LogIndex(1), node.status().lastLogIndex)
        assertEquals(Term(3), node.status().lastLogTerm)

        node.stop()
    }

    @Test
    fun restartedFollowerDoesNotGrantSecondVoteInSameTerm() = runBlocking {
        val storage = InMemoryStableStorage(
            PersistentState(
                currentTerm = Term(4),
                votedFor = NodeId("candidate-a"),
                log = listOf(LogEntry(LogIndex(1), Term(4), "existing")),
            ),
        )
        val node = newNode("a", emptySet(), storage, InMemoryTransport(), NeverElectionScheduler)

        node.start()
        val response = node.requestVote(
            RequestVoteRequest(
                term = Term(4),
                candidateId = NodeId("candidate-b"),
                lastLogIndex = LogIndex(1),
                lastLogTerm = Term(4),
            ),
        )

        assertEquals(Term(4), response.term)
        assertTrue(!response.voteGranted)
        assertEquals(NodeId("candidate-a"), storage.load().votedFor)

        node.stop()
    }

    private fun newNode(
        id: String,
        peers: Set<NodeId>,
        storage: InMemoryStableStorage<String> = InMemoryStableStorage(),
        service: RaftService = InMemoryTransport(),
        scheduler: RaftScheduler = NeverElectionScheduler,
        stateMachine: StateMachine<String, String> = RecordingStateMachine(),
        coroutineContext: CoroutineContext = kotlinx.coroutines.Dispatchers.Default,
    ): CoroutineRaftNode<String, String> =
        CoroutineRaftNode(
            config = ClusterConfig(NodeId(id), peers, 25.milliseconds),
            storage = storage,
            service = service,
            codec = StringCommandCodec,
            stateMachine = stateMachine,
            scheduler = scheduler,
            coroutineContext = coroutineContext,
        )
}

private object StringCommandCodec : CommandCodec<String> {
    override fun encode(command: String): ByteArray = command.encodeToByteArray()
    override fun decode(bytes: ByteArray): String = bytes.decodeToString()
}

private class RecordingStateMachine : StateMachine<String, String> {
    val applied = mutableListOf<String>()

    override suspend fun apply(command: String): String {
        applied += command
        return "applied:$command"
    }
}

private object FailingStateMachine : StateMachine<String, String> {
    override suspend fun apply(command: String): String {
        error("boom")
    }
}

private object FastElectionScheduler : RaftScheduler {
    override suspend fun delay(duration: Duration) {
        kotlinx.coroutines.delay(duration)
    }

    override fun nextElectionTimeout(): Duration = 10.milliseconds
}

private object SlowElectionScheduler : RaftScheduler {
    override suspend fun delay(duration: Duration) {
        kotlinx.coroutines.delay(duration)
    }

    override fun nextElectionTimeout(): Duration = 500.milliseconds
}

private object MediumElectionScheduler : RaftScheduler {
    override suspend fun delay(duration: Duration) {
        kotlinx.coroutines.delay(duration)
    }

    override fun nextElectionTimeout(): Duration = 120.milliseconds
}

private class OneShotElectionScheduler : RaftScheduler {
    private var electionTimeouts = 0

    override suspend fun delay(duration: Duration) {
        kotlinx.coroutines.delay(duration)
    }

    override fun nextElectionTimeout(): Duration {
        electionTimeouts += 1
        return if (electionTimeouts == 1) 10.milliseconds else 10_000.milliseconds
    }
}

private object NeverElectionScheduler : RaftScheduler {
    override suspend fun delay(duration: Duration) {
        kotlinx.coroutines.delay(10_000)
    }

    override fun nextElectionTimeout(): Duration = 10_000.milliseconds
}

private class HigherTermOnAppendService : RaftService {
    override suspend fun requestVote(
        target: NodeId,
        request: RequestVoteRequest,
    ): RequestVoteResponse = RequestVoteResponse(request.term, voteGranted = true)

    override suspend fun appendEntries(
        target: NodeId,
        request: AppendEntriesRequest,
    ): AppendEntriesResponse = AppendEntriesResponse(Term(request.term.value + 1), success = false)
}
