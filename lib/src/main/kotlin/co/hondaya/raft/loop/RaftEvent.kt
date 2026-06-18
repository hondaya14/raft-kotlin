package co.hondaya.raft.loop

import co.hondaya.raft.LogIndex
import co.hondaya.raft.NodeId
import co.hondaya.raft.SubmitResult
import co.hondaya.raft.Term
import co.hondaya.raft.transport.AppendEntriesRequest
import co.hondaya.raft.transport.AppendEntriesResponse
import co.hondaya.raft.transport.RequestVoteRequest
import co.hondaya.raft.transport.RequestVoteResponse
import kotlinx.coroutines.CompletableDeferred

internal sealed interface RaftEvent<out C : Any, out R : Any> {
    fun observedTerm(): Term? = null

    data class AppendEntriesReceived(
        val request: AppendEntriesRequest,
        val reply: CompletableDeferred<AppendEntriesResponse>,
    ) : RaftEvent<Nothing, Nothing> {
        override fun observedTerm(): Term = request.term
    }

    data class RequestVoteReceived(
        val request: RequestVoteRequest,
        val reply: CompletableDeferred<RequestVoteResponse>,
    ) : RaftEvent<Nothing, Nothing> {
        override fun observedTerm(): Term = request.term
    }

    data class AppendEntriesResponseReceived(
        val from: NodeId,
        val request: AppendEntriesRequest,
        val response: AppendEntriesResponse,
    ) : RaftEvent<Nothing, Nothing> {
        override fun observedTerm(): Term = response.term
    }

    data class RequestVoteResponseReceived(
        val from: NodeId,
        val term: Term,
        val voteGranted: Boolean,
    ) : RaftEvent<Nothing, Nothing> {
        override fun observedTerm(): Term = term
    }

    data object ElectionTimeout : RaftEvent<Nothing, Nothing>
    data object HeartbeatTick : RaftEvent<Nothing, Nothing>

    data class ClientSubmit<C : Any, R : Any>(
        val command: C,
        val reply: CompletableDeferred<SubmitResult<R>>,
    ) : RaftEvent<C, R>

    data class Applied<R : Any>(
        val through: LogIndex,
        val term: Term,
        val result: R?,
    ) : RaftEvent<Nothing, R>

    data class ApplyFailed(val reason: String) : RaftEvent<Nothing, Nothing>
    data class Stop(val reply: CompletableDeferred<Unit>) : RaftEvent<Nothing, Nothing>
}
