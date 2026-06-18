package co.hondaya.raft.cluster

@JvmInline
value class NodeId(val value: String) {
    init {
        require(value.isNotBlank()) { "node id must not be blank" }
    }
}
