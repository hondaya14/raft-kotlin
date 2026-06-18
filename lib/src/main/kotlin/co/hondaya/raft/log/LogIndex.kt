package co.hondaya.raft.log

@JvmInline
value class LogIndex(val value: Long) : Comparable<LogIndex> {
    init {
        require(value >= 0) { "log index must be non-negative" }
    }

    override fun compareTo(other: LogIndex): Int = value.compareTo(other.value)

    operator fun plus(delta: Long): LogIndex = LogIndex(value + delta)

    operator fun minus(delta: Long): LogIndex = LogIndex(value - delta)
}
