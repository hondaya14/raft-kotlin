package co.hondaya.raft.transport

import co.hondaya.raft.LogIndex
import co.hondaya.raft.NodeId
import co.hondaya.raft.Term
import kotlinx.coroutines.delay
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlin.time.Duration

data class WireLogEntry(
    val index: LogIndex,
    val term: Term,
    val command: ByteArray,
    val noOp: Boolean = false,
) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is WireLogEntry) return false
        return index == other.index && term == other.term && noOp == other.noOp && command.contentEquals(other.command)
    }

    override fun hashCode(): Int {
        var result = index.hashCode()
        result = 31 * result + term.hashCode()
        result = 31 * result + command.contentHashCode()
        result = 31 * result + noOp.hashCode()
        return result
    }
}

data class RequestVoteRequest(
    val term: Term,
    val candidateId: NodeId,
    val lastLogIndex: LogIndex,
    val lastLogTerm: Term,
)

data class RequestVoteResponse(
    val term: Term,
    val voteGranted: Boolean,
)

data class AppendEntriesRequest(
    val term: Term,
    val leaderId: NodeId,
    val prevLogIndex: LogIndex,
    val prevLogTerm: Term,
    val entries: List<WireLogEntry>,
    val leaderCommit: LogIndex,
)

data class AppendEntriesResponse(
    val term: Term,
    val success: Boolean,
)

interface RaftService {
    suspend fun requestVote(target: NodeId, request: RequestVoteRequest): RequestVoteResponse
    suspend fun appendEntries(target: NodeId, request: AppendEntriesRequest): AppendEntriesResponse
}

interface RaftPeerEndpoint {
    suspend fun requestVote(request: RequestVoteRequest): RequestVoteResponse
    suspend fun appendEntries(request: AppendEntriesRequest): AppendEntriesResponse
}

class InMemoryTransport : RaftService {
    private val mutex = Mutex()
    private val endpoints = linkedMapOf<NodeId, RaftPeerEndpoint>()
    private val blocked = mutableSetOf<Pair<NodeId, NodeId>>()
    private val delays = linkedMapOf<Pair<NodeId, NodeId>, Duration>()
    private val duplicateDeliveries = linkedMapOf<Pair<NodeId, NodeId>, Int>()

    suspend fun register(id: NodeId, endpoint: RaftPeerEndpoint) {
        mutex.withLock {
            endpoints[id] = endpoint
        }
    }

    suspend fun unregister(id: NodeId) {
        mutex.withLock {
            endpoints.remove(id)
            blocked.removeAll { it.first == id || it.second == id }
            delays.keys
                .filter { it.first == id || it.second == id }
                .forEach { delays.remove(it) }
            duplicateDeliveries.keys
                .filter { it.first == id || it.second == id }
                .forEach { duplicateDeliveries.remove(it) }
        }
    }

    suspend fun disconnect(from: NodeId, to: NodeId) {
        mutex.withLock {
            blocked += from to to
        }
    }

    suspend fun reconnect(from: NodeId, to: NodeId) {
        mutex.withLock {
            blocked -= from to to
        }
    }

    suspend fun delayLink(from: NodeId, to: NodeId, duration: Duration) {
        mutex.withLock {
            if (duration == Duration.ZERO) {
                delays.remove(from to to)
            } else {
                require(duration.isPositive()) { "delay must be positive or zero" }
                delays[from to to] = duration
            }
        }
    }

    suspend fun clearDelay(from: NodeId, to: NodeId) {
        mutex.withLock {
            delays.remove(from to to)
        }
    }

    suspend fun duplicateLink(from: NodeId, to: NodeId, deliveries: Int) {
        require(deliveries >= 1) { "deliveries must be at least 1" }
        mutex.withLock {
            if (deliveries == 1) {
                duplicateDeliveries.remove(from to to)
            } else {
                duplicateDeliveries[from to to] = deliveries
            }
        }
    }

    suspend fun clearDuplicate(from: NodeId, to: NodeId) {
        mutex.withLock {
            duplicateDeliveries.remove(from to to)
        }
    }

    override suspend fun requestVote(
        target: NodeId,
        request: RequestVoteRequest,
    ): RequestVoteResponse {
        val route = routeFor(request.candidateId, target) ?: return RequestVoteResponse(request.term, false)
        return route.deliver { requestVote(request) }
    }

    override suspend fun appendEntries(
        target: NodeId,
        request: AppendEntriesRequest,
    ): AppendEntriesResponse {
        val route = routeFor(request.leaderId, target) ?: return AppendEntriesResponse(request.term, false)
        return route.deliver { appendEntries(request) }
    }

    private suspend fun routeFor(from: NodeId, target: NodeId): Route? = mutex.withLock {
        if (from to target in blocked) {
            null
        } else {
            endpoints[target]?.let { Route(it, delays[from to target], duplicateDeliveries[from to target] ?: 1) }
        }
    }

    private data class Route(
        val endpoint: RaftPeerEndpoint,
        val delay: Duration?,
        val deliveries: Int,
    ) {
        suspend fun <R> deliver(call: suspend RaftPeerEndpoint.() -> R): R {
            var firstResponse: R? = null
            repeat(deliveries) { delivery ->
                delay?.let { delay(it) }
                val response = endpoint.call()
                if (delivery == 0) {
                    firstResponse = response
                }
            }
            @Suppress("UNCHECKED_CAST")
            return firstResponse as R
        }
    }
}
