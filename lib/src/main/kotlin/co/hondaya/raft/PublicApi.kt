package co.hondaya.raft

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

/**
 * Converts commands between the application type and the bytes stored in Raft log entries.
 */
interface CommandCodec<C : Any> {
    /** Encodes an application command for replication and durable storage. */
    fun encode(command: C): ByteArray

    /** Decodes a command received from replicated log bytes. */
    fun decode(bytes: ByteArray): C
}

/** Application state machine driven by committed Raft log entries. */
interface StateMachine<C : Any, R : Any> {
    /** Applies a committed command and returns the command result. */
    suspend fun apply(command: C): R
}

/**
 * Point-in-time status snapshot for a Raft node.
 *
 * @property id node identity.
 * @property state current Raft role.
 * @property currentTerm latest term observed by this node.
 * @property votedFor candidate that received this node's vote in [currentTerm], if any.
 * @property leaderId latest known leader, or `null` when unknown.
 * @property commitIndex highest log index known to be committed.
 * @property lastApplied highest committed log index applied to the state machine.
 * @property lastLogIndex highest log index stored locally.
 * @property lastLogTerm term of [lastLogIndex].
 */
data class RaftStatus(
    val id: NodeId,
    val state: NodeState,
    val currentTerm: Term,
    val votedFor: NodeId?,
    val leaderId: NodeId?,
    val commitIndex: LogIndex,
    val lastApplied: LogIndex,
    val lastLogIndex: LogIndex,
    val lastLogTerm: Term,
)
