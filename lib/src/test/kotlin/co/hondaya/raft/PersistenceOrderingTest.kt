package co.hondaya.raft

import co.hondaya.raft.cluster.*
import co.hondaya.raft.command.*
import co.hondaya.raft.node.*
import co.hondaya.raft.loop.CoroutineRaftNode
import co.hondaya.raft.log.*
import co.hondaya.raft.scheduler.RaftScheduler
import co.hondaya.raft.storage.InMemoryStableStorage
import co.hondaya.raft.storage.PersistentState
import co.hondaya.raft.storage.StableStorage
import co.hondaya.raft.transport.AppendEntriesRequest
import co.hondaya.raft.transport.AppendEntriesResponse
import co.hondaya.raft.transport.RaftService
import co.hondaya.raft.transport.RequestVoteRequest
import co.hondaya.raft.transport.RequestVoteResponse
import co.hondaya.raft.transport.WireLogEntry
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlin.coroutines.CoroutineContext
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds

class PersistenceOrderingTest {
    @Test
    fun grantedVoteWaitsForTermAndVotePersistence() = runBlocking {
        val storage = BlockingStableStorage<String>(blockSave = true)
        val node = newNode("a", emptySet(), storage = storage)

        node.start()
        val vote = async {
            node.requestVote(
                RequestVoteRequest(Term(1), NodeId("candidate"), LogIndex(0), Term(0)),
            )
        }
        withTimeout(500.milliseconds) { storage.saveStarted.await() }

        assertTrue(!vote.isCompleted)
        storage.releaseSave()

        assertEquals(RequestVoteResponse(Term(1), voteGranted = true), vote.await())
        node.stop()
    }

    @Test
    fun appendEntriesSuccessWaitsForAppendPersistence() = runBlocking {
        val storage = BlockingStableStorage<String>(blockAppend = { entries -> entries.any { !it.noOp } })
        val node = newNode("a", emptySet(), storage = storage)

        node.start()
        val append = async {
            node.appendEntries(
                AppendEntriesRequest(
                    term = Term(1),
                    leaderId = NodeId("leader"),
                    prevLogIndex = LogIndex(0),
                    prevLogTerm = Term(0),
                    entries = listOf(WireLogEntry(LogIndex(1), Term(1), "cmd".encodeToByteArray())),
                    leaderCommit = LogIndex(0),
                ),
            )
        }
        withTimeout(500.milliseconds) { storage.appendStarted.await() }

        assertTrue(!append.isCompleted)
        storage.releaseAppend()

        assertEquals(AppendEntriesResponse(Term(1), success = true), append.await())
        node.stop()
    }

    @Test
    fun truncationWaitsForTruncatePersistenceBeforeReply() = runBlocking {
        val storage = BlockingStableStorage(
            initialState = PersistentState(
                currentTerm = Term(2),
                log = listOf(
                    LogEntry(LogIndex(1), Term(1), "stable"),
                    LogEntry(LogIndex(2), Term(2), "old"),
                ),
            ),
            blockTruncate = true,
        )
        val node = newNode("a", emptySet(), storage = storage)

        node.start()
        val append = async {
            node.appendEntries(
                AppendEntriesRequest(
                    term = Term(3),
                    leaderId = NodeId("leader"),
                    prevLogIndex = LogIndex(1),
                    prevLogTerm = Term(1),
                    entries = listOf(WireLogEntry(LogIndex(2), Term(3), "new".encodeToByteArray())),
                    leaderCommit = LogIndex(0),
                ),
            )
        }
        withTimeout(500.milliseconds) { storage.truncateStarted.await() }

        assertTrue(!append.isCompleted)
        storage.releaseTruncate()

        assertEquals(AppendEntriesResponse(Term(3), success = true), append.await())
        node.stop()
    }

    @Test
    fun higherTermReplyWaitsForTermPersistence() = runBlocking {
        val storage = BlockingStableStorage<String>(
            initialState = PersistentState(currentTerm = Term(1)),
            blockSave = true,
        )
        val node = newNode("a", emptySet(), storage = storage)

        node.start()
        val append = async {
            node.appendEntries(
                AppendEntriesRequest(
                    term = Term(2),
                    leaderId = NodeId("leader"),
                    prevLogIndex = LogIndex(0),
                    prevLogTerm = Term(0),
                    entries = emptyList(),
                    leaderCommit = LogIndex(0),
                ),
            )
        }
        withTimeout(500.milliseconds) { storage.saveStarted.await() }

        assertTrue(!append.isCompleted)
        storage.releaseSave()

        assertEquals(AppendEntriesResponse(Term(2), success = true), append.await())
        node.stop()
    }

    @Test
    fun leaderDoesNotReplicateClientEntryBeforeAppendPersistence() = runBlocking {
        val storage = BlockingStableStorage<String>(
            blockAppend = { entries -> entries.any { !it.noOp } },
        )
        val service = RecordingRaftService()
        val node = newNode(
            id = "a",
            peers = setOf(NodeId("b"), NodeId("c")),
            storage = storage,
            service = service,
            scheduler = OrderingFastElectionScheduler,
        )

        node.start()
        withTimeout(1_000.milliseconds) {
            while (node.status().state != NodeState.LEADER || node.status().lastLogIndex < LogIndex(1)) {
                delay(10)
            }
        }
        service.clear()

        val submit = async { node.submit("client-command") }
        withTimeout(500.milliseconds) { storage.appendStarted.await() }

        assertTrue(service.appendRequests.none { request ->
            request.entries.any { !it.noOp && it.command.decodeToString() == "client-command" }
        })

        storage.releaseAppend()
        withTimeout(1_000.milliseconds) {
            while (service.appendRequests.none { request ->
                    request.entries.any { !it.noOp && it.command.decodeToString() == "client-command" }
                }
            ) {
                delay(10)
            }
        }
        submit.cancel()
        node.stop()
    }

    private fun newNode(
        id: String,
        peers: Set<NodeId>,
        storage: StableStorage<String>,
        service: RaftService = RecordingRaftService(),
        scheduler: RaftScheduler = OrderingNeverElectionScheduler,
        coroutineContext: CoroutineContext = kotlinx.coroutines.Dispatchers.Default,
    ): CoroutineRaftNode<String, String> =
        CoroutineRaftNode(
            config = ClusterConfig(NodeId(id), peers, 25.milliseconds),
            storage = storage,
            service = service,
            codec = OrderingStringCommandCodec,
            stateMachine = OrderingRecordingStateMachine(),
            scheduler = scheduler,
            coroutineContext = coroutineContext,
        )
}

private object OrderingStringCommandCodec : CommandCodec<String> {
    override fun encode(command: String): ByteArray = command.encodeToByteArray()
    override fun decode(bytes: ByteArray): String = bytes.decodeToString()
}

private class OrderingRecordingStateMachine : StateMachine<String, String> {
    override suspend fun apply(command: String): String = "applied:$command"
}

private object OrderingFastElectionScheduler : RaftScheduler {
    override suspend fun delay(duration: Duration) {
        kotlinx.coroutines.delay(duration)
    }

    override fun nextElectionTimeout(): Duration = 10.milliseconds
}

private object OrderingNeverElectionScheduler : RaftScheduler {
    override suspend fun delay(duration: Duration) {
        kotlinx.coroutines.delay(10_000)
    }

    override fun nextElectionTimeout(): Duration = 10_000.milliseconds
}

private class BlockingStableStorage<C : Any>(
    initialState: PersistentState<C> = PersistentState(),
    private val blockSave: Boolean = false,
    private val blockAppend: (List<LogEntry<C>>) -> Boolean = { false },
    private val blockTruncate: Boolean = false,
) : StableStorage<C> {
    private val delegate = InMemoryStableStorage(initialState)
    val saveStarted = CompletableDeferred<Unit>()
    val appendStarted = CompletableDeferred<Unit>()
    val truncateStarted = CompletableDeferred<Unit>()
    private val saveReleased = CompletableDeferred<Unit>()
    private val appendReleased = CompletableDeferred<Unit>()
    private val truncateReleased = CompletableDeferred<Unit>()

    override suspend fun load(): PersistentState<C> = delegate.load()

    override suspend fun saveTermAndVote(currentTerm: Term, votedFor: NodeId?) {
        if (blockSave && !saveStarted.isCompleted) {
            saveStarted.complete(Unit)
            saveReleased.await()
        }
        delegate.saveTermAndVote(currentTerm, votedFor)
    }

    override suspend fun appendEntries(entries: List<LogEntry<C>>) {
        if (blockAppend(entries) && !appendStarted.isCompleted) {
            appendStarted.complete(Unit)
            appendReleased.await()
        }
        delegate.appendEntries(entries)
    }

    override suspend fun truncateSuffix(fromIndex: LogIndex) {
        if (blockTruncate && !truncateStarted.isCompleted) {
            truncateStarted.complete(Unit)
            truncateReleased.await()
        }
        delegate.truncateSuffix(fromIndex)
    }

    override suspend fun replaceLog(entries: List<LogEntry<C>>) {
        delegate.replaceLog(entries)
    }

    fun releaseSave() {
        saveReleased.complete(Unit)
    }

    fun releaseAppend() {
        appendReleased.complete(Unit)
    }

    fun releaseTruncate() {
        truncateReleased.complete(Unit)
    }
}

private class RecordingRaftService : RaftService {
    private val _appendRequests = mutableListOf<AppendEntriesRequest>()
    val appendRequests: List<AppendEntriesRequest>
        get() = synchronized(_appendRequests) { _appendRequests.toList() }

    override suspend fun requestVote(
        target: NodeId,
        request: RequestVoteRequest,
    ): RequestVoteResponse = RequestVoteResponse(request.term, voteGranted = true)

    override suspend fun appendEntries(
        target: NodeId,
        request: AppendEntriesRequest,
    ): AppendEntriesResponse {
        synchronized(_appendRequests) {
            _appendRequests += request
        }
        return AppendEntriesResponse(request.term, success = true)
    }

    fun clear() {
        synchronized(_appendRequests) {
            _appendRequests.clear()
        }
    }
}
