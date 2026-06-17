package co.hondaya.raft

import kotlin.time.Duration

@JvmInline
value class NodeId(val value: String) {
    init {
        require(value.isNotBlank()) { "node id must not be blank" }
    }
}

@JvmInline
value class Term(val value: Long) : Comparable<Term> {
    init {
        require(value >= 0) { "term must be non-negative" }
    }

    override fun compareTo(other: Term): Int = value.compareTo(other.value)

    operator fun plus(delta: Long): Term = Term(value + delta)
}

@JvmInline
value class LogIndex(val value: Long) : Comparable<LogIndex> {
    init {
        require(value >= 0) { "log index must be non-negative" }
    }

    override fun compareTo(other: LogIndex): Int = value.compareTo(other.value)

    operator fun plus(delta: Long): LogIndex = LogIndex(value + delta)

    operator fun minus(delta: Long): LogIndex = LogIndex(value - delta)
}

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

data class ClusterConfig(
    val selfId: NodeId,
    val peers: Set<NodeId>,
    val heartbeatInterval: Duration,
) {
    init {
        require(selfId !in peers) { "self id must not be present in peers" }
        require(heartbeatInterval.isPositive()) { "heartbeat interval must be positive" }
    }

    val clusterSize: Int = peers.size + 1
    val majority: Int = (clusterSize / 2) + 1
}
