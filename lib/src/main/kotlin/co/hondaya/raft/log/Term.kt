package co.hondaya.raft.log

@JvmInline
value class Term(val value: Long) : Comparable<Term> {
    init {
        require(value >= 0) { "term must be non-negative" }
    }

    override fun compareTo(other: Term): Int = value.compareTo(other.value)

    operator fun plus(delta: Long): Term = Term(value + delta)
}
