package co.hondaya.raft.loop

import co.hondaya.raft.log.LogIndex
import co.hondaya.raft.cluster.NodeId
import co.hondaya.raft.node.SubmitResult
import co.hondaya.raft.log.Term
import co.hondaya.raft.transport.AppendEntriesRequest
import co.hondaya.raft.transport.AppendEntriesResponse
import co.hondaya.raft.transport.RequestVoteRequest
import co.hondaya.raft.transport.RequestVoteResponse
import kotlinx.coroutines.CompletableDeferred

/**
 * Internal message consumed by the Raft event loop.
 *
 * These events are not Raft paper wire types. They adapt RPC arrivals, RPC
 * responses, timers, client calls, state-machine apply feedback, and lifecycle
 * requests into one serial queue so mutable Raft state is updated in one place.
 */
internal sealed interface RaftEvent<out C : Any, out R : Any> {
    /**
     * Returns a term observed by this event before the event-specific handler runs.
     *
     * The event loop uses this hook to apply the Raft "newer term means step
     * down" rule consistently for both inbound RPCs and outbound RPC responses.
     */
    fun observedTerm(): Term? = null

    /**
     * Inbound `AppendEntries` RPC sent by a leader or candidate's discovered leader.
     *
     * @property rpcRequest RPC request received from a peer.
     * @property rpcReply completion used to return the RPC response to that peer.
     */
    data class AppendEntriesReceived(
        val rpcRequest: AppendEntriesRequest,
        val rpcReply: CompletableDeferred<AppendEntriesResponse>,
    ) : RaftEvent<Nothing, Nothing> {
        override fun observedTerm(): Term = rpcRequest.term
    }

    /**
     * Inbound `RequestVote` RPC sent by a candidate.
     *
     * @property rpcRequest RPC request received from a peer.
     * @property rpcReply completion used to return the vote response to that peer.
     */
    data class RequestVoteReceived(
        val rpcRequest: RequestVoteRequest,
        val rpcReply: CompletableDeferred<RequestVoteResponse>,
    ) : RaftEvent<Nothing, Nothing> {
        override fun observedTerm(): Term = rpcRequest.term
    }

    /**
     * Response to an `AppendEntries` RPC previously sent by this node as leader.
     *
     * @property respondedBy peer that returned the response.
     * @property sentRequest original request sent to the peer.
     * @property rpcResponse response returned by the peer.
     */
    data class AppendEntriesResponseReceived(
        val respondedBy: NodeId,
        val sentRequest: AppendEntriesRequest,
        val rpcResponse: AppendEntriesResponse,
    ) : RaftEvent<Nothing, Nothing> {
        override fun observedTerm(): Term = rpcResponse.term
    }

    /**
     * Response to a `RequestVote` RPC previously sent by this node as candidate.
     *
     * @property respondedBy peer that returned the vote response.
     * @property responseTerm term reported by the responding peer.
     * @property grantedVote true when the peer granted its vote to this node.
     */
    data class RequestVoteResponseReceived(
        val respondedBy: NodeId,
        val responseTerm: Term,
        val grantedVote: Boolean,
    ) : RaftEvent<Nothing, Nothing> {
        override fun observedTerm(): Term = responseTerm
    }

    /** Internal timer event indicating that this node should start an election. */
    data object ElectionTimeout : RaftEvent<Nothing, Nothing>

    /** Internal leader tick requesting a fresh heartbeat round. */
    data object HeartbeatTick : RaftEvent<Nothing, Nothing>

    /**
     * Client command submitted through the public node API.
     *
     * @property submittedCommand command to append and replicate if this node is leader.
     * @property submitResult completion used to resume the suspended submit caller.
     */
    data class ClientSubmit<C : Any, R : Any>(
        val submittedCommand: C,
        val submitResult: CompletableDeferred<SubmitResult<R>>,
    ) : RaftEvent<C, R>

    /**
     * Feedback from the state-machine apply loop after a committed entry is applied.
     *
     * @property appliedThrough highest log index applied by this event.
     * @property appliedTerm term of the applied log entry.
     * @property applyResult state-machine result, or `null` for no-op entries.
     */
    data class Applied<R : Any>(
        val appliedThrough: LogIndex,
        val appliedTerm: Term,
        val applyResult: R?,
    ) : RaftEvent<Nothing, R>

    /**
     * State-machine apply failure that should fail pending submissions and stop the node.
     *
     * @property failureReason human-readable failure reason.
     */
    data class ApplyFailed(val failureReason: String) : RaftEvent<Nothing, Nothing>

    /**
     * Lifecycle request to stop the node through the Raft event loop.
     *
     * @property stopped completion signaled after shutdown finishes.
     */
    data class Stop(val stopped: CompletableDeferred<Unit>) : RaftEvent<Nothing, Nothing>
}
