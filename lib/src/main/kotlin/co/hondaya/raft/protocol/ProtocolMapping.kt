package co.hondaya.raft.protocol

import co.hondaya.raft.LogIndex
import co.hondaya.raft.NodeId
import co.hondaya.raft.Term
import co.hondaya.raft.protocol.v1.AppendEntriesRequest
import co.hondaya.raft.protocol.v1.AppendEntriesRequestInternal
import co.hondaya.raft.protocol.v1.AppendEntriesResponse
import co.hondaya.raft.protocol.v1.AppendEntriesResponseInternal
import co.hondaya.raft.protocol.v1.LogEntry
import co.hondaya.raft.protocol.v1.LogEntryInternal
import co.hondaya.raft.protocol.v1.RequestVoteRequest
import co.hondaya.raft.protocol.v1.RequestVoteRequestInternal
import co.hondaya.raft.protocol.v1.RequestVoteResponse
import co.hondaya.raft.protocol.v1.RequestVoteResponseInternal
import co.hondaya.raft.transport.AppendEntriesRequest as DomainAppendEntriesRequest
import co.hondaya.raft.transport.AppendEntriesResponse as DomainAppendEntriesResponse
import co.hondaya.raft.transport.RequestVoteRequest as DomainRequestVoteRequest
import co.hondaya.raft.transport.RequestVoteResponse as DomainRequestVoteResponse
import co.hondaya.raft.transport.WireLogEntry as DomainWireLogEntry
import kotlinx.io.bytestring.ByteString

fun DomainRequestVoteRequest.toProto(): RequestVoteRequest =
    RequestVoteRequestInternal().apply {
        term = this@toProto.term.value.toULong()
        candidateId = this@toProto.candidateId.value
        lastLogIndex = this@toProto.lastLogIndex.value.toULong()
        lastLogTerm = this@toProto.lastLogTerm.value.toULong()
    }

fun RequestVoteRequest.toDomain(): DomainRequestVoteRequest =
    DomainRequestVoteRequest(
        term = Term(term.toLong()),
        candidateId = NodeId(candidateId),
        lastLogIndex = LogIndex(lastLogIndex.toLong()),
        lastLogTerm = Term(lastLogTerm.toLong()),
    )

fun DomainRequestVoteResponse.toProto(): RequestVoteResponse =
    RequestVoteResponseInternal().apply {
        term = this@toProto.term.value.toULong()
        voteGranted = this@toProto.voteGranted
    }

fun RequestVoteResponse.toDomain(): DomainRequestVoteResponse =
    DomainRequestVoteResponse(
        term = Term(term.toLong()),
        voteGranted = voteGranted,
    )

fun DomainAppendEntriesRequest.toProto(): AppendEntriesRequest =
    AppendEntriesRequestInternal().apply {
        term = this@toProto.term.value.toULong()
        leaderId = this@toProto.leaderId.value
        prevLogIndex = this@toProto.prevLogIndex.value.toULong()
        prevLogTerm = this@toProto.prevLogTerm.value.toULong()
        entries = this@toProto.entries.map { it.toProto() }
        leaderCommit = this@toProto.leaderCommit.value.toULong()
    }

fun AppendEntriesRequest.toDomain(): DomainAppendEntriesRequest =
    DomainAppendEntriesRequest(
        term = Term(term.toLong()),
        leaderId = NodeId(leaderId),
        prevLogIndex = LogIndex(prevLogIndex.toLong()),
        prevLogTerm = Term(prevLogTerm.toLong()),
        entries = entries.map { it.toDomain() },
        leaderCommit = LogIndex(leaderCommit.toLong()),
    )

fun DomainAppendEntriesResponse.toProto(): AppendEntriesResponse =
    AppendEntriesResponseInternal().apply {
        term = this@toProto.term.value.toULong()
        success = this@toProto.success
    }

fun AppendEntriesResponse.toDomain(): DomainAppendEntriesResponse =
    DomainAppendEntriesResponse(
        term = Term(term.toLong()),
        success = success,
    )

fun DomainWireLogEntry.toProto(): LogEntry =
    LogEntryInternal().apply {
        index = this@toProto.index.value.toULong()
        term = this@toProto.term.value.toULong()
        command = ByteString(this@toProto.command)
        noOp = this@toProto.noOp
    }

fun LogEntry.toDomain(): DomainWireLogEntry =
    DomainWireLogEntry(
        index = LogIndex(index.toLong()),
        term = Term(term.toLong()),
        command = command.toByteArray(),
        noOp = noOp,
    )
