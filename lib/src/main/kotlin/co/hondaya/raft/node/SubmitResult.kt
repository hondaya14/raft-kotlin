package co.hondaya.raft.node

import co.hondaya.raft.log.LogIndex
import co.hondaya.raft.cluster.NodeId
import co.hondaya.raft.log.Term

/** Result returned after attempting to submit a command to a node. */
sealed interface SubmitResult<out R> {
    /**
     * The command was committed and applied to the state machine.
     *
     * @property index log index of the applied command.
     * @property term term of the applied log entry.
     * @property result state machine result for the command.
     */
    data class Applied<R>(
        val index: LogIndex,
        val term: Term,
        val result: R,
    ) : SubmitResult<R>

    /**
     * The contacted node is not the leader.
     *
     * @property leaderId latest known leader, or `null` when this node does not know one.
     */
    data class NotLeader(val leaderId: NodeId?) : SubmitResult<Nothing>

    /**
     * The command could not be processed by this node.
     *
     * @property reason human-readable explanation of the failure.
     */
    data class Unavailable(val reason: String) : SubmitResult<Nothing>
}
