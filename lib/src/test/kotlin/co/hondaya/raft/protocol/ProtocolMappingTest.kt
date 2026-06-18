package co.hondaya.raft.protocol

import co.hondaya.raft.log.LogIndex
import co.hondaya.raft.cluster.NodeId
import co.hondaya.raft.log.Term
import co.hondaya.raft.transport.AppendEntriesRequest
import co.hondaya.raft.transport.AppendEntriesResponse
import co.hondaya.raft.transport.RequestVoteRequest
import co.hondaya.raft.transport.RequestVoteResponse
import co.hondaya.raft.transport.WireLogEntry
import kotlin.test.Test
import kotlin.test.assertEquals

class ProtocolMappingTest {
    @Test
    fun requestVoteRoundTripsThroughGeneratedProto() {
        val request = RequestVoteRequest(
            term = Term(7),
            candidateId = NodeId("candidate"),
            lastLogIndex = LogIndex(11),
            lastLogTerm = Term(6),
        )
        val response = RequestVoteResponse(Term(8), voteGranted = true)

        assertEquals(request, request.toProto().toDomain())
        assertEquals(response, response.toProto().toDomain())
    }

    @Test
    fun appendEntriesRoundTripsThroughGeneratedProto() {
        val request = AppendEntriesRequest(
            term = Term(4),
            leaderId = NodeId("leader"),
            prevLogIndex = LogIndex(1),
            prevLogTerm = Term(3),
            entries = listOf(
                WireLogEntry(LogIndex(2), Term(4), "cmd".encodeToByteArray()),
                WireLogEntry(LogIndex(3), Term(4), "noop".encodeToByteArray(), noOp = true),
            ),
            leaderCommit = LogIndex(2),
        )
        val response = AppendEntriesResponse(Term(5), success = false)

        assertEquals(request, request.toProto().toDomain())
        assertEquals(response, response.toProto().toDomain())
    }
}

