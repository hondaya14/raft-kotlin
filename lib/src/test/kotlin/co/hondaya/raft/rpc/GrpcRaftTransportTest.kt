package co.hondaya.raft.rpc

import co.hondaya.raft.cluster.NodeId
import co.hondaya.raft.log.LogIndex
import co.hondaya.raft.log.Term
import co.hondaya.raft.transport.AppendEntriesRequest
import co.hondaya.raft.transport.AppendEntriesResponse
import co.hondaya.raft.transport.RaftPeerEndpoint
import co.hondaya.raft.transport.RequestVoteRequest
import co.hondaya.raft.transport.RequestVoteResponse
import co.hondaya.raft.transport.WireLogEntry
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals

class GrpcRaftTransportTest {
    @Test
    fun clientCallsReachServerEndpointOverGrpc() = runBlocking {
        val endpoint = RecordingGrpcEndpoint()
        val server = GrpcRaftServer(port = 0, endpoint = endpoint).start()
        val target = NodeId("server")
        val service = GrpcRaftService(
            mapOf(target to GrpcPeerAddress("127.0.0.1", server.boundPort)),
        )

        try {
            val voteRequest = RequestVoteRequest(Term(7), NodeId("candidate"), LogIndex(11), Term(6))
            val voteResponse = service.requestVote(target, voteRequest)

            assertEquals(RequestVoteResponse(Term(8), voteGranted = true), voteResponse)
            assertEquals(voteRequest, endpoint.lastVoteRequest)

            val appendRequest = AppendEntriesRequest(
                term = Term(8),
                leaderId = NodeId("leader"),
                prevLogIndex = LogIndex(11),
                prevLogTerm = Term(6),
                entries = listOf(WireLogEntry(LogIndex(12), Term(8), "cmd".encodeToByteArray())),
                leaderCommit = LogIndex(12),
            )
            val appendResponse = service.appendEntries(target, appendRequest)

            assertEquals(AppendEntriesResponse(Term(8), success = true), appendResponse)
            assertEquals(appendRequest, endpoint.lastAppendRequest)
            assertContentEquals("cmd".encodeToByteArray(), endpoint.lastAppendRequest?.entries?.single()?.command)
        } finally {
            service.close()
            server.close()
        }
    }
}

private class RecordingGrpcEndpoint : RaftPeerEndpoint {
    var lastVoteRequest: RequestVoteRequest? = null
        private set
    var lastAppendRequest: AppendEntriesRequest? = null
        private set

    override suspend fun requestVote(request: RequestVoteRequest): RequestVoteResponse {
        lastVoteRequest = request
        return RequestVoteResponse(Term(request.term.value + 1), voteGranted = true)
    }

    override suspend fun appendEntries(request: AppendEntriesRequest): AppendEntriesResponse {
        lastAppendRequest = request
        return AppendEntriesResponse(request.term, success = true)
    }
}
