package co.hondaya.raft

interface RaftNode<C : Any, R : Any> {
    val id: NodeId

    suspend fun start()
    suspend fun stop()
    suspend fun submit(command: C): SubmitResult<R>
    fun status(): RaftStatus
}

sealed interface SubmitResult<out R> {
    data class Applied<R>(
        val index: LogIndex,
        val term: Term,
        val result: R,
    ) : SubmitResult<R>

    data class NotLeader(val leaderId: NodeId?) : SubmitResult<Nothing>
    data class Unavailable(val reason: String) : SubmitResult<Nothing>
}

interface CommandCodec<C : Any> {
    fun encode(command: C): ByteArray
    fun decode(bytes: ByteArray): C
}

interface StateMachine<C : Any, R : Any> {
    suspend fun apply(command: C): R
}

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

