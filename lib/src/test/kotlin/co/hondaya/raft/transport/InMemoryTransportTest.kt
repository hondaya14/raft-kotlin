package co.hondaya.raft.transport

import co.hondaya.raft.log.LogIndex
import co.hondaya.raft.cluster.NodeId
import co.hondaya.raft.log.Term
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.milliseconds

class InMemoryTransportTest {
    @Test
    fun delayedLinkDelaysDelivery() = runBlocking {
        val transport = InMemoryTransport()
        val endpoint = RecordingEndpoint("b")
        transport.register(NodeId("b"), endpoint)
        transport.delayLink(NodeId("a"), NodeId("b"), 50.milliseconds)

        val startedAt = System.nanoTime()
        transport.appendEntries(NodeId("b"), appendEntriesFrom("a"))
        val elapsedMillis = (System.nanoTime() - startedAt) / 1_000_000

        assertEquals(listOf("b"), endpoint.arrivals)
        assert(elapsedMillis >= 40) { "expected delayed delivery, elapsed=${elapsedMillis}ms" }
    }

    @Test
    fun delayedLinksCanReorderConcurrentDelivery() = runBlocking {
        val arrivals = mutableListOf<String>()
        val transport = InMemoryTransport()
        transport.register(NodeId("b"), RecordingEndpoint("b", arrivals))
        transport.register(NodeId("c"), RecordingEndpoint("c", arrivals))
        transport.delayLink(NodeId("a"), NodeId("b"), 80.milliseconds)

        val delayed = async { transport.appendEntries(NodeId("b"), appendEntriesFrom("a")) }
        delay(10)
        val immediate = async { transport.appendEntries(NodeId("c"), appendEntriesFrom("a")) }

        immediate.await()
        delayed.await()

        assertEquals(listOf("c", "b"), arrivals)
    }

    @Test
    fun duplicateLinkDeliversSameRpcMultipleTimes() = runBlocking {
        val endpoint = RecordingEndpoint("b")
        val transport = InMemoryTransport()
        transport.register(NodeId("b"), endpoint)
        transport.duplicateLink(NodeId("a"), NodeId("b"), deliveries = 3)

        transport.appendEntries(NodeId("b"), appendEntriesFrom("a"))

        assertEquals(listOf("b", "b", "b"), endpoint.arrivals)
    }

    private fun appendEntriesFrom(leaderId: String): AppendEntriesRequest =
        AppendEntriesRequest(
            term = Term(1),
            leaderId = NodeId(leaderId),
            prevLogIndex = LogIndex(0),
            prevLogTerm = Term(0),
            entries = emptyList(),
            leaderCommit = LogIndex(0),
        )
}

private class RecordingEndpoint(
    private val label: String,
    private val sharedArrivals: MutableList<String> = mutableListOf(),
) : RaftPeerEndpoint {
    val arrivals: List<String>
        get() = sharedArrivals.toList()

    override suspend fun requestVote(request: RequestVoteRequest): RequestVoteResponse {
        sharedArrivals += label
        return RequestVoteResponse(request.term, voteGranted = true)
    }

    override suspend fun appendEntries(request: AppendEntriesRequest): AppendEntriesResponse {
        sharedArrivals += label
        return AppendEntriesResponse(request.term, success = true)
    }
}
