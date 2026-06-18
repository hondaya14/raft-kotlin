package co.hondaya.raft.node

import co.hondaya.raft.cluster.NodeId

/**
 * A Raft node that accepts client commands and participates in a cluster.
 *
 * @param C command type submitted by clients and replicated through the log.
 * @param R result type produced by the state machine after a command is applied.
 */
interface RaftNode<C : Any, R : Any> {
    /** Stable identity of this node within the cluster configuration. */
    val id: NodeId

    /** Starts the node event loop and begins participating in elections and replication. */
    suspend fun start()

    /** Stops the node and releases its running coroutines. */
    suspend fun stop()

    /**
     * Submits a command to the cluster.
     *
     * The command is accepted only by a leader. Followers return [SubmitResult.NotLeader]
     * with the latest known leader hint when available.
     */
    suspend fun submit(command: C): SubmitResult<R>

    /** Returns a snapshot of the node's latest observed Raft status. */
    fun status(): RaftStatus
}
