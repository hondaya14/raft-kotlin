package co.hondaya.raft.rpc

import co.hondaya.raft.log.LogIndex
import co.hondaya.raft.cluster.NodeId
import co.hondaya.raft.log.Term
import co.hondaya.raft.protocol.toDomain
import co.hondaya.raft.protocol.toProto
import co.hondaya.raft.transport.AppendEntriesRequest
import co.hondaya.raft.transport.AppendEntriesResponse
import co.hondaya.raft.transport.RaftPeerEndpoint
import co.hondaya.raft.transport.RequestVoteRequest
import co.hondaya.raft.transport.RequestVoteResponse
import co.hondaya.raft.transport.WireLogEntry
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import co.hondaya.raft.protocol.v1.AppendEntriesRequest as AppendEntriesRequestProto
import co.hondaya.raft.protocol.v1.AppendEntriesResponse as AppendEntriesResponseProto
import co.hondaya.raft.protocol.v1.RaftService as ProtoRaftService
import co.hondaya.raft.protocol.v1.RequestVoteRequest as RequestVoteRequestProto
import co.hondaya.raft.protocol.v1.RequestVoteResponse as RequestVoteResponseProto

class KotlinxRpcAdaptersTest {
    @Test
    fun endpointAdapterForwardsGeneratedRpcCallsToRaftEndpoint() = runBlocking {
        val voteRequest = RequestVoteRequest(Term(3), NodeId("candidate"), LogIndex(4), Term(2))
        val appendRequest = AppendEntriesRequest(
            term = Term(3),
            leaderId = NodeId("leader"),
            prevLogIndex = LogIndex(4),
            prevLogTerm = Term(2),
            entries = listOf(WireLogEntry(LogIndex(5), Term(3), "cmd".encodeToByteArray())),
            leaderCommit = LogIndex(4),
        )
        val endpoint = RecordingEndpoint(voteRequest, appendRequest)
        val adapter = KotlinxRpcRaftEndpointAdapter(endpoint)

        assertEquals(RequestVoteResponse(Term(3), true), adapter.RequestVote(voteRequest.toProto()).toDomain())
        assertEquals(AppendEntriesResponse(Term(3), true), adapter.AppendEntries(appendRequest.toProto()).toDomain())
        assertEquals(voteRequest, endpoint.lastVoteRequest)
        assertEquals(appendRequest, endpoint.lastAppendRequest)
    }

    @Test
    fun serviceAdapterForwardsCoreCallsToGeneratedPeer() = runBlocking {
        val target = NodeId("b")
        val peer = RecordingProtoPeer()
        val adapter = KotlinxRpcRaftServiceAdapter(mapOf(target to peer))
        val voteRequest = RequestVoteRequest(Term(5), NodeId("a"), LogIndex(2), Term(4))
        val appendRequest = AppendEntriesRequest(
            term = Term(5),
            leaderId = NodeId("a"),
            prevLogIndex = LogIndex(2),
            prevLogTerm = Term(4),
            entries = listOf(WireLogEntry(LogIndex(3), Term(5), "cmd".encodeToByteArray(), noOp = true)),
            leaderCommit = LogIndex(3),
        )

        assertEquals(RequestVoteResponse(Term(6), false), adapter.requestVote(target, voteRequest))
        assertEquals(AppendEntriesResponse(Term(6), false), adapter.appendEntries(target, appendRequest))
        assertEquals(voteRequest, peer.lastVoteRequest?.toDomain())
        assertEquals(appendRequest, peer.lastAppendRequest?.toDomain())
    }
}

private class RecordingEndpoint(
    private val expectedVoteRequest: RequestVoteRequest,
    private val expectedAppendRequest: AppendEntriesRequest,
) : RaftPeerEndpoint {
    var lastVoteRequest: RequestVoteRequest? = null
        private set
    var lastAppendRequest: AppendEntriesRequest? = null
        private set

    override suspend fun requestVote(request: RequestVoteRequest): RequestVoteResponse {
        lastVoteRequest = request
        assertEquals(expectedVoteRequest, request)
        return RequestVoteResponse(request.term, voteGranted = true)
    }

    override suspend fun appendEntries(request: AppendEntriesRequest): AppendEntriesResponse {
        lastAppendRequest = request
        assertEquals(expectedAppendRequest, request)
        return AppendEntriesResponse(request.term, success = true)
    }
}

private class RecordingProtoPeer : ProtoRaftService {
    var lastVoteRequest: RequestVoteRequestProto? = null
        private set
    var lastAppendRequest: AppendEntriesRequestProto? = null
        private set

    override suspend fun RequestVote(message: RequestVoteRequestProto): RequestVoteResponseProto {
        lastVoteRequest = message
        return RequestVoteResponse(Term(6), voteGranted = false).toProto()
    }

    override suspend fun AppendEntries(message: AppendEntriesRequestProto): AppendEntriesResponseProto {
        lastAppendRequest = message
        return AppendEntriesResponse(Term(6), success = false).toProto()
    }
}
