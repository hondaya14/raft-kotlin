package co.hondaya.raft.log

data class LogEntry<C : Any>(
    val index: LogIndex,
    val term: Term,
    val command: C?,
    val noOp: Boolean = false,
) {
    init {
        require(noOp || command != null) { "non-no-op entries must contain a command" }
    }
}
